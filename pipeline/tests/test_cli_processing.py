from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from pipeline.cli import run
from pipeline.resource import EpisodeGroup, MediaFile, MediaGroup, SourceOption


class _FakeToken:
    def get_secret_value(self) -> str:
        return "token"


class _FakeFrameioSettings:
    token = _FakeToken()
    destination_id = "destination"


class CliProcessingOrderTests(unittest.TestCase):
    def test_run_processes_each_group_in_sequence(self) -> None:
        source = SourceOption(name="AudioHijack", path=Path("."), selected=True)
        day_file_one = MediaFile(
            source="AudioHijack",
            path=Path("/audio/input_1.wav"),
            created_at=datetime(2026, 2, 16, 12, 17, 33),
        )
        day_file_two = MediaFile(
            source="AudioHijack",
            path=Path("/audio/input_2.wav"),
            created_at=datetime(2026, 2, 16, 12, 28, 0),
        )
        group_one = MediaGroup(
            start_time=datetime(2026, 2, 16, 12, 17, 33), files=[day_file_one]
        )
        group_two = MediaGroup(
            start_time=datetime(2026, 2, 16, 12, 28, 0), files=[day_file_two]
        )
        groups = [group_one, group_two]

        events: list[tuple[str, str]] = []
        moved_episode_groups = [
            EpisodeGroup(
                episode_dir=Path("/podcast/episode_46"),
                start_time=group_one.start_time,
                files=[
                    MediaFile(
                        source="AudioHijack",
                        path=Path("/podcast/episode_46/input.wav"),
                        created_at=group_one.start_time,
                    )
                ],
            ),
            EpisodeGroup(
                episode_dir=Path("/podcast/episode_47"),
                start_time=group_two.start_time,
                files=[
                    MediaFile(
                        source="AudioHijack",
                        path=Path("/podcast/episode_47/input.wav"),
                        created_at=group_two.start_time,
                    )
                ],
            ),
        ]
        move_call_count = {"value": 0}

        def move_side_effect(
            grouped: list[MediaGroup], podcast_root: Path, dry_run: bool = False
        ) -> list[EpisodeGroup]:
            self.assertFalse(dry_run)
            self.assertEqual(podcast_root, Path("/podcast"))
            self.assertEqual(len(grouped), 1)
            events.append(("move", grouped[0].start_time.strftime("%H:%M:%S")))
            index = move_call_count["value"]
            move_call_count["value"] += 1
            return [moved_episode_groups[index]]

        def convert_side_effect(
            episode_groups: list[EpisodeGroup], dry_run: bool = False
        ) -> int:
            self.assertFalse(dry_run)
            self.assertEqual(len(episode_groups), 1)
            events.append(("convert", episode_groups[0].episode_dir.name))
            return 0

        def upload_side_effect(
            episode_groups: list[EpisodeGroup], token: str, destination_id: str
        ) -> int:
            self.assertEqual(token, "token")
            self.assertEqual(destination_id, "destination")
            self.assertEqual(len(episode_groups), 1)
            events.append(("upload", episode_groups[0].episode_dir.name))
            return 0

        def delete_side_effect(paths: list[Path]) -> tuple[int, list[str]]:
            events.append(("delete", str(len(paths))))
            return len(paths), []

        with (
            patch(
                "pipeline.cli.load_frameio_settings",
                return_value=_FakeFrameioSettings(),
            ),
            patch("pipeline.cli.discover_sources", return_value=[source]),
            patch(
                "pipeline.cli.scan_sources_for_today_files",
                return_value=[day_file_one, day_file_two],
            ),
            patch("pipeline.cli.group_files_by_start_time", return_value=groups),
            patch("pipeline.cli.align_groups_by_media_duration", return_value={}),
            patch("pipeline.cli.print_group_table", return_value=None),
            patch("pipeline.cli.find_existing_episode_numbers", return_value=[45]),
            patch("pipeline.cli.sys.stdin.isatty", return_value=True),
            patch("pipeline.cli.Confirm.ask", return_value=True) as confirm_mock,
            patch(
                "pipeline.cli.move_groups_to_episodes", side_effect=move_side_effect
            ) as move_mock,
            patch(
                "pipeline.cli.convert_mov_files", side_effect=convert_side_effect
            ) as convert_mock,
            patch(
                "pipeline.cli.upload_episode_files_to_frameio",
                side_effect=upload_side_effect,
            ) as upload_mock,
            patch(
                "pipeline.cli.delete_original_media_files",
                side_effect=delete_side_effect,
            ) as delete_mock,
        ):
            exit_code = run(
                start_window=timedelta(minutes=5),
                podcast_root=Path("/podcast"),
                audiohijack_path=Path("/audio"),
                all_volumes=False,
                yes=True,
                dry_run=False,
                skip_frameio_upload=False,
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(move_mock.call_count, 2)
        self.assertEqual(convert_mock.call_count, 2)
        self.assertEqual(upload_mock.call_count, 2)
        confirm_mock.assert_called_once()
        delete_mock.assert_called_once()
        delete_paths = delete_mock.call_args.args[0]
        self.assertEqual(
            set(delete_paths), {Path("/audio/input_1.wav"), Path("/audio/input_2.wav")}
        )
        self.assertEqual(
            events,
            [
                ("move", "12:17:33"),
                ("convert", "episode_46"),
                ("upload", "episode_46"),
                ("move", "12:28:00"),
                ("convert", "episode_47"),
                ("upload", "episode_47"),
                ("delete", "2"),
            ],
        )

    def test_run_dry_run_stays_bulk_and_skips_convert_upload(self) -> None:
        source = SourceOption(name="AudioHijack", path=Path("."), selected=True)
        day_file = MediaFile(
            source="AudioHijack",
            path=Path("/audio/input.wav"),
            created_at=datetime(2026, 2, 16, 12, 17, 33),
        )
        groups = [
            MediaGroup(start_time=datetime(2026, 2, 16, 12, 17, 33), files=[day_file]),
            MediaGroup(start_time=datetime(2026, 2, 16, 12, 28, 0), files=[day_file]),
        ]

        with (
            patch(
                "pipeline.cli.load_frameio_settings",
                return_value=_FakeFrameioSettings(),
            ),
            patch("pipeline.cli.discover_sources", return_value=[source]),
            patch("pipeline.cli.scan_sources_for_today_files", return_value=[day_file]),
            patch("pipeline.cli.group_files_by_start_time", return_value=groups),
            patch("pipeline.cli.align_groups_by_media_duration", return_value={}),
            patch("pipeline.cli.print_group_table", return_value=None),
            patch("pipeline.cli.find_existing_episode_numbers", return_value=[45]),
            patch("pipeline.cli.Confirm.ask", return_value=True) as confirm_mock,
            patch("pipeline.cli.move_groups_to_episodes", return_value=[]) as move_mock,
            patch("pipeline.cli.convert_mov_files", return_value=0) as convert_mock,
            patch(
                "pipeline.cli.upload_episode_files_to_frameio", return_value=0
            ) as upload_mock,
            patch(
                "pipeline.cli.delete_original_media_files", return_value=(0, [])
            ) as delete_mock,
        ):
            exit_code = run(
                start_window=timedelta(minutes=5),
                podcast_root=Path("/podcast"),
                audiohijack_path=Path("/audio"),
                all_volumes=False,
                yes=True,
                dry_run=True,
                skip_frameio_upload=False,
            )

        self.assertEqual(exit_code, 0)
        move_mock.assert_called_once_with(groups, Path("/podcast"), dry_run=True)
        convert_mock.assert_not_called()
        upload_mock.assert_not_called()
        confirm_mock.assert_not_called()
        delete_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
