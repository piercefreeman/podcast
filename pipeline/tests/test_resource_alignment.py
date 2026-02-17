from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from rich.progress import Progress

from pipeline.resource import (
    MediaFile,
    SourceOption,
    align_groups_by_media_duration,
    group_files_by_start_time,
    scan_source_for_today_files,
)


class ResourceAlignmentTests(unittest.TestCase):
    def test_scan_source_for_today_files_ignores_non_media_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            mov_file = root / "clip.mov"
            dat_file = root / "FFXFER.DAT"
            mov_file.touch()
            dat_file.touch()

            day_start = datetime(2026, 2, 16)
            day_end = day_start + timedelta(days=1)

            created_times = {
                mov_file: datetime(2026, 2, 16, 12, 0, 0),
                dat_file: datetime(2026, 2, 16, 19, 0, 0),
            }

            source = SourceOption(name="EOS_DIGITAL", path=root, selected=True)
            with patch(
                "pipeline.resource.safe_created_at",
                side_effect=lambda path: created_times[path],
            ):
                with Progress(disable=True) as progress:
                    task_id = progress.add_task("scan", total=None)
                    matches = scan_source_for_today_files(
                        source, day_start, day_end, progress, task_id
                    )

            self.assertEqual([file.path for file in matches], [mov_file])

    def test_align_groups_by_media_duration_includes_spillover_and_skewed_sources(
        self,
    ) -> None:
        day_start = datetime(2026, 2, 16)
        day_end = day_start + timedelta(days=1)

        audio_root = Path("/audio")
        eos_root = Path("/eos")
        ninja_root = Path("/ninjav")

        a1 = audio_root / "session_1217.wav"
        a2 = audio_root / "session_1228.wav"
        a3 = audio_root / "session_1455.wav"
        a4 = audio_root / "session_1632.wav"

        eos_0008 = eos_root / "DCIM/101_FUJI/DSCF0008.MOV"
        eos_0009 = eos_root / "DCIM/101_FUJI/DSCF0009.MOV"
        eos_0010 = eos_root / "DCIM/101_FUJI/DSCF0010.MOV"
        eos_dat = eos_root / "FFDB/FFXFER.DAT"

        ninja_832 = ninja_root / "NINJAV_S001_S001_T832.MOV"
        ninja_833 = ninja_root / "NINJAV_S001_S001_T833.MOV"
        ninja_834 = ninja_root / "NINJAV_S001_S001_T834.MOV"

        dt_1217 = datetime(2026, 2, 16, 12, 17, 33)
        dt_1228 = datetime(2026, 2, 16, 12, 28, 0)
        dt_1455 = datetime(2026, 2, 16, 14, 55, 52)
        dt_1632 = datetime(2026, 2, 16, 16, 32, 12)
        dt_2248 = datetime(2026, 2, 16, 22, 48, 34)

        day_matched_files = [
            MediaFile(source="AudioHijack", path=a1, created_at=dt_1217),
            MediaFile(source="AudioHijack", path=a2, created_at=dt_1228),
            MediaFile(source="AudioHijack", path=a3, created_at=dt_1455),
            MediaFile(source="AudioHijack", path=a4, created_at=dt_1632),
            # Only one EOS file is in the strict day window.
            MediaFile(source="EOS_DIGITAL", path=eos_0008, created_at=dt_2248),
        ]

        groups = group_files_by_start_time(day_matched_files, timedelta(minutes=5))

        selected_sources = [
            SourceOption(name="AudioHijack", path=audio_root, selected=True),
            SourceOption(name="EOS_DIGITAL", path=eos_root, selected=True),
            SourceOption(name="NINJAV", path=ninja_root, selected=True),
        ]

        created_times = {
            eos_0008: dt_2248,
            eos_0009: datetime(2026, 2, 17, 0, 22, 46),
            eos_0010: datetime(2026, 2, 17, 0, 57, 52),
            eos_dat: datetime(2026, 2, 16, 19, 36, 44),
            ninja_832: datetime(2029, 11, 30, 23, 13, 48),
            ninja_833: datetime(2029, 12, 1, 1, 50, 26),
            ninja_834: datetime(2029, 12, 1, 3, 26, 44),
        }

        durations = {
            a1: 588.8,
            a2: 8338.60,
            a3: 5116.94,
            a4: 1447.34,
            eos_0008: 8888.88,
            eos_0009: 5147.14,
            eos_0010: 1473.47,
            ninja_832: 8891.95,
            ninja_833: 5144.01,
            ninja_834: 1474.87,
        }

        iter_files_map = {
            eos_root: [eos_0008, eos_0009, eos_0010, eos_dat],
            ninja_root: [ninja_832, ninja_833, ninja_834],
        }

        def fake_iter_files(root: Path):
            for file_path in iter_files_map.get(root, []):
                yield file_path

        with (
            patch("pipeline.resource.iter_files", side_effect=fake_iter_files),
            patch(
                "pipeline.resource.safe_created_at",
                side_effect=lambda path: created_times.get(path),
            ),
            patch(
                "pipeline.resource.safe_media_duration_seconds",
                side_effect=lambda path: durations.get(path),
            ),
        ):
            outside_day = align_groups_by_media_duration(
                groups, selected_sources, day_matched_files, day_start, day_end
            )

        self.assertEqual(outside_day, {"EOS_DIGITAL": 2, "NINJAV": 3})

        self.assertEqual(
            [group.start_time for group in groups], [dt_1217, dt_1228, dt_1455, dt_1632]
        )

        groups_by_start = {group.start_time: group for group in groups}
        self.assertEqual(
            [
                file.path.name
                for file in groups_by_start[dt_1228].files
                if file.source == "EOS_DIGITAL"
            ],
            ["DSCF0008.MOV"],
        )
        self.assertEqual(
            [
                file.path.name
                for file in groups_by_start[dt_1455].files
                if file.source == "EOS_DIGITAL"
            ],
            ["DSCF0009.MOV"],
        )
        self.assertEqual(
            [
                file.path.name
                for file in groups_by_start[dt_1632].files
                if file.source == "EOS_DIGITAL"
            ],
            ["DSCF0010.MOV"],
        )

        self.assertEqual(
            [
                file.path.name
                for file in groups_by_start[dt_1228].files
                if file.source == "NINJAV"
            ],
            ["NINJAV_S001_S001_T832.MOV"],
        )
        self.assertEqual(
            [
                file.path.name
                for file in groups_by_start[dt_1455].files
                if file.source == "NINJAV"
            ],
            ["NINJAV_S001_S001_T833.MOV"],
        )
        self.assertEqual(
            [
                file.path.name
                for file in groups_by_start[dt_1632].files
                if file.source == "NINJAV"
            ],
            ["NINJAV_S001_S001_T834.MOV"],
        )

    def test_group_files_by_start_time_keeps_multiple_files_from_same_source(
        self,
    ) -> None:
        anchor = datetime(2026, 2, 16, 12, 28, 0)
        files = [
            MediaFile(
                source="AudioHijack", path=Path("/audio/stem_1.wav"), created_at=anchor
            ),
            MediaFile(
                source="AudioHijack", path=Path("/audio/stem_2.wav"), created_at=anchor
            ),
            MediaFile(
                source="AudioHijack", path=Path("/audio/stem_3.wav"), created_at=anchor
            ),
            MediaFile(
                source="AudioHijack", path=Path("/audio/stem_4.wav"), created_at=anchor
            ),
            MediaFile(
                source="NINJAV",
                path=Path("/ninjav/NINJAV_S001_S001_T832.MOV"),
                created_at=anchor + timedelta(seconds=30),
            ),
        ]

        groups = group_files_by_start_time(files, timedelta(minutes=5))
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0].files), 5)
        self.assertEqual(
            sorted(file.source for file in groups[0].files),
            ["AudioHijack", "AudioHijack", "AudioHijack", "AudioHijack", "NINJAV"],
        )


if __name__ == "__main__":
    unittest.main()
