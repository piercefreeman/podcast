from __future__ import annotations

import os
import shutil
import subprocess
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from .config import AUDIO_EXTENSIONS, EPISODE_PATTERN

console = Console()


@dataclass
class SourceOption:
    name: str
    path: Path
    selected: bool = False


@dataclass(frozen=True)
class MediaFile:
    source: str
    path: Path
    created_at: datetime


@dataclass
class MediaGroup:
    start_time: datetime
    files: list[MediaFile]


@dataclass
class EpisodeGroup:
    episode_dir: Path
    start_time: datetime
    files: list[MediaFile]


MEDIA_DURATION_EXTENSIONS = AUDIO_EXTENSIONS | {
    ".avi",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mxf",
}
SPILLOVER_WINDOW = timedelta(hours=6)


def discover_sources(
    audiohijack_path: Path, volumes_root: Path = Path("/Volumes")
) -> list[SourceOption]:
    options = [SourceOption(name="AudioHijack", path=audiohijack_path, selected=True)]

    if volumes_root.exists():
        for entry in sorted(volumes_root.iterdir(), key=lambda path: path.name.lower()):
            if not entry.is_dir():
                continue
            options.append(SourceOption(name=entry.name, path=entry, selected=False))

    return options


def iter_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if not name.startswith(".")]
        for filename in filenames:
            if filename.startswith("."):
                continue
            yield Path(dirpath) / filename


def safe_created_at(path: Path) -> datetime | None:
    try:
        stat = path.stat()
    except OSError:
        return None

    created_ts = getattr(stat, "st_birthtime", stat.st_mtime)
    return datetime.fromtimestamp(created_ts)


def is_media_file(path: Path) -> bool:
    return path.suffix.lower() in MEDIA_DURATION_EXTENSIONS


@lru_cache(maxsize=4096)
def safe_media_duration_seconds(path: Path) -> float | None:
    if not is_media_file(path):
        return None
    if shutil.which("ffprobe") is None:
        return None

    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    if completed.returncode != 0:
        return None

    raw_duration = completed.stdout.strip()
    if not raw_duration:
        return None

    try:
        duration = float(raw_duration)
    except ValueError:
        return None

    if duration <= 0:
        return None
    return duration


def scan_source_for_today_files(
    source: SourceOption,
    day_start: datetime,
    day_end: datetime,
    progress: Progress,
    task_id: int,
) -> list[MediaFile]:
    checked_since_update = 0
    matches: list[MediaFile] = []

    for file_path in iter_files(source.path):
        if not is_media_file(file_path):
            continue
        checked_since_update += 1
        if checked_since_update >= 50:
            progress.update(task_id, advance=checked_since_update)
            checked_since_update = 0

        created_at = safe_created_at(file_path)
        if created_at is None:
            continue
        if day_start <= created_at < day_end:
            matches.append(
                MediaFile(
                    source=source.name,
                    path=file_path,
                    created_at=created_at,
                )
            )

    if checked_since_update:
        progress.update(task_id, advance=checked_since_update)

    return matches


def scan_sources_for_today_files(
    sources: list[SourceOption],
    day_start: datetime,
    day_end: datetime,
) -> list[MediaFile]:
    all_files: list[MediaFile] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed:,.0f} scanned"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        overall_task = progress.add_task("Completed sources", total=len(sources))
        for source in sources:
            task_id = progress.add_task(f"Scanning {source.name}", total=None)
            try:
                source_files = scan_source_for_today_files(
                    source,
                    day_start,
                    day_end,
                    progress,
                    task_id,
                )
                all_files.extend(source_files)
                progress.update(
                    task_id,
                    description=f"Scanned {source.name}: {len(source_files)} matches",
                )
            except Exception as exc:
                progress.update(task_id, description=f"Failed {source.name}: {exc}")
            finally:
                progress.stop_task(task_id)
                progress.advance(overall_task, 1)

    return sorted(all_files, key=lambda media: media.created_at)


def align_groups_by_media_duration(
    groups: list[MediaGroup],
    selected_sources: list[SourceOption],
    day_matched_files: list[MediaFile],
    day_start: datetime,
    day_end: datetime,
) -> dict[str, int]:
    if not groups:
        return {}

    def _is_better_state(
        left: tuple[int, float, float], right: tuple[int, float, float] | None
    ) -> bool:
        if right is None:
            return True
        if left[0] != right[0]:
            return left[0] > right[0]
        if left[1] != right[1]:
            return left[1] < right[1]
        return left[2] < right[2]

    def _score_candidate_to_anchor(
        media_file: MediaFile,
        duration: float,
        anchor_index: int,
        group_anchor_times: list[datetime],
        group_targets: list[float | None],
    ) -> tuple[float, float] | None:
        target_duration = group_targets[anchor_index]
        if target_duration is None:
            return None
        duration_delta = abs(duration - target_duration)
        max_allowed_delta = max(5.0, target_duration * 0.25)
        if duration_delta > max_allowed_delta:
            return None

        # Duration drives matching; datetime breaks ties for candidates in one source.
        time_delta = abs(
            (media_file.created_at - group_anchor_times[anchor_index]).total_seconds()
        )
        return duration_delta, time_delta

    def _select_monotonic_matches(
        candidates: list[MediaFile],
        allowed_anchor_indices: list[int],
        group_anchor_times: list[datetime],
        group_targets: list[float | None],
    ) -> list[tuple[int, MediaFile]]:
        if not candidates or not allowed_anchor_indices:
            return []

        sorted_candidates = sorted(candidates, key=lambda item: item.created_at)
        matchable_candidates: list[tuple[MediaFile, float]] = []
        for media_file in sorted_candidates:
            duration = safe_media_duration_seconds(media_file.path)
            if duration is None:
                continue
            matchable_candidates.append((media_file, duration))

        candidate_count = len(matchable_candidates)
        anchor_count = len(allowed_anchor_indices)
        if candidate_count == 0 or anchor_count == 0:
            return []

        dp: list[list[tuple[int, float, float] | None]] = [
            [None] * (anchor_count + 1) for _ in range(candidate_count + 1)
        ]
        prev: list[list[tuple[str, int, int] | None]] = [
            [None] * (anchor_count + 1) for _ in range(candidate_count + 1)
        ]
        dp[0][0] = (0, 0.0, 0.0)

        for candidate_index in range(candidate_count + 1):
            for anchor_pos in range(anchor_count + 1):
                state = dp[candidate_index][anchor_pos]
                if state is None:
                    continue

                if candidate_index < candidate_count:
                    next_state = state
                    existing = dp[candidate_index + 1][anchor_pos]
                    if _is_better_state(next_state, existing):
                        dp[candidate_index + 1][anchor_pos] = next_state
                        prev[candidate_index + 1][anchor_pos] = (
                            "skip_candidate",
                            candidate_index,
                            anchor_pos,
                        )

                if anchor_pos < anchor_count:
                    next_state = state
                    existing = dp[candidate_index][anchor_pos + 1]
                    if _is_better_state(next_state, existing):
                        dp[candidate_index][anchor_pos + 1] = next_state
                        prev[candidate_index][anchor_pos + 1] = (
                            "skip_anchor",
                            candidate_index,
                            anchor_pos,
                        )

                if candidate_index < candidate_count and anchor_pos < anchor_count:
                    media_file, duration = matchable_candidates[candidate_index]
                    anchor_index = allowed_anchor_indices[anchor_pos]
                    score = _score_candidate_to_anchor(
                        media_file,
                        duration,
                        anchor_index,
                        group_anchor_times,
                        group_targets,
                    )
                    if score is None:
                        continue
                    next_state = (
                        state[0] + 1,
                        state[1] + score[0],
                        state[2] + score[1],
                    )
                    existing = dp[candidate_index + 1][anchor_pos + 1]
                    if _is_better_state(next_state, existing):
                        dp[candidate_index + 1][anchor_pos + 1] = next_state
                        prev[candidate_index + 1][anchor_pos + 1] = (
                            "match",
                            candidate_index,
                            anchor_pos,
                        )

        matches: list[tuple[int, MediaFile]] = []
        candidate_index = candidate_count
        anchor_pos = anchor_count
        while candidate_index > 0 or anchor_pos > 0:
            step = prev[candidate_index][anchor_pos]
            if step is None:
                break
            action, previous_candidate_index, previous_anchor_pos = step
            if action == "match":
                anchor_index = allowed_anchor_indices[previous_anchor_pos]
                media_file, _ = matchable_candidates[previous_candidate_index]
                matches.append((anchor_index, media_file))
            candidate_index = previous_candidate_index
            anchor_pos = previous_anchor_pos

        matches.reverse()
        return matches

    day_files_by_source: dict[str, list[MediaFile]] = {}
    for media_file in day_matched_files:
        day_files_by_source.setdefault(media_file.source, []).append(media_file)

    primary_source_name = None
    if day_files_by_source:
        primary_source_name = max(
            day_files_by_source.items(), key=lambda item: len(item[1])
        )[0]
    if primary_source_name is None:
        return {}

    # Keep only primary-source files in scaffold groups; everything else gets
    # reattached by duration to these primary anchors.
    for group in groups:
        group.files = [
            file for file in group.files if file.source == primary_source_name
        ]
    groups[:] = [group for group in groups if group.files]
    groups.sort(key=lambda group: group.start_time)
    if not groups:
        return {}

    group_anchor_times = [group.start_time for group in groups]
    group_targets: list[float | None] = []
    for group in groups:
        durations = [
            duration
            for duration in (
                safe_media_duration_seconds(media_file.path)
                for media_file in group.files
            )
            if duration is not None
        ]
        group_targets.append(max(durations) if durations else None)
    if all(target is None for target in group_targets):
        return {}

    primary_anchor_indices = [
        index for index, target in enumerate(group_targets) if target is not None
    ]

    spillover_start = day_start - SPILLOVER_WINDOW
    spillover_end = day_end + SPILLOVER_WINDOW

    matches_from_outside_day_filter: dict[str, int] = {}
    for source in selected_sources:
        if source.name == primary_source_name:
            continue

        source_day_files = day_files_by_source.get(source.name, [])

        candidate_by_path: dict[Path, MediaFile] = {
            media_file.path: media_file for media_file in source_day_files
        }

        # Also include near-midnight spillover files around the selected day.
        for file_path in iter_files(source.path):
            if not is_media_file(file_path):
                continue
            created_at = safe_created_at(file_path)
            if created_at is None:
                continue

            if source_day_files:
                if created_at < spillover_start or created_at >= spillover_end:
                    continue

            candidate_by_path.setdefault(
                file_path,
                MediaFile(source=source.name, path=file_path, created_at=created_at),
            )

        all_candidates = list(candidate_by_path.values())
        monotonic_matches = _select_monotonic_matches(
            all_candidates,
            primary_anchor_indices,
            group_anchor_times,
            group_targets,
        )
        for anchor_index, media_file in monotonic_matches:
            groups[anchor_index].files.append(media_file)
        outside_matches = [
            media_file
            for _, media_file in monotonic_matches
            if media_file.created_at < day_start or media_file.created_at >= day_end
        ]
        if outside_matches:
            matches_from_outside_day_filter[source.name] = len(outside_matches)

    for group in groups:
        group.files.sort(key=lambda item: item.created_at)

    groups[:] = [group for group in groups if group.files]
    groups.sort(key=lambda group: group.start_time)
    return matches_from_outside_day_filter


def group_files_by_start_time(
    files: list[MediaFile], window: timedelta
) -> list[MediaGroup]:
    if not files:
        return []

    sorted_files = sorted(files, key=lambda item: item.created_at)
    groups: list[MediaGroup] = []
    window_seconds = window.total_seconds()

    current_group: list[MediaFile] = [sorted_files[0]]
    current_anchor = sorted_files[0].created_at

    for media_file in sorted_files[1:]:
        delta = (media_file.created_at - current_anchor).total_seconds()
        if delta <= window_seconds:
            current_group.append(media_file)
            continue

        groups.append(
            MediaGroup(
                start_time=current_anchor,
                files=sorted(current_group, key=lambda item: item.created_at),
            )
        )
        current_group = [media_file]
        current_anchor = media_file.created_at

    groups.append(
        MediaGroup(
            start_time=current_anchor,
            files=sorted(current_group, key=lambda item: item.created_at),
        )
    )
    return groups


def find_existing_episode_numbers(podcast_root: Path) -> list[int]:
    if not podcast_root.exists():
        return []

    numbers: list[int] = []
    for entry in podcast_root.iterdir():
        if not entry.is_dir():
            continue
        match = EPISODE_PATTERN.match(entry.name)
        if match:
            numbers.append(int(match.group(1)))

    return sorted(numbers)


def allocate_episode_directories(podcast_root: Path, group_count: int) -> list[Path]:
    podcast_root.mkdir(parents=True, exist_ok=True)
    existing = find_existing_episode_numbers(podcast_root)
    next_number = (existing[-1] + 1) if existing else 1

    episode_directories: list[Path] = []
    while len(episode_directories) < group_count:
        candidate = podcast_root / f"episode_{next_number}"
        if not candidate.exists():
            episode_directories.append(candidate)
        next_number += 1

    return episode_directories


def copy_file_to_destination(source_path: Path, destination: Path) -> Path:
    shutil.copy2(str(source_path), str(destination))
    return destination


def plan_group_destinations(files: list[MediaFile], episode_dir: Path) -> list[Path]:
    planned: list[Path] = []
    reserved_paths: set[Path] = set()

    for media_file in files:
        candidate = episode_dir / media_file.path.name
        if candidate.exists() or candidate in reserved_paths:
            stem = candidate.stem
            suffix = candidate.suffix
            counter = 1
            while True:
                next_candidate = candidate.with_name(f"{stem}_{counter}{suffix}")
                if not next_candidate.exists() and next_candidate not in reserved_paths:
                    candidate = next_candidate
                    break
                counter += 1

        reserved_paths.add(candidate)
        planned.append(candidate)

    return planned


def move_groups_to_episodes(
    groups: list[MediaGroup],
    podcast_root: Path,
    dry_run: bool = False,
) -> list[EpisodeGroup]:
    episode_directories = allocate_episode_directories(podcast_root, len(groups))
    total_files = sum(len(group.files) for group in groups)
    episode_groups = [
        EpisodeGroup(
            episode_dir=episode_dir,
            start_time=group.start_time,
            files=[],
        )
        for group, episode_dir in zip(groups, episode_directories)
    ]

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold green]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed:,.0f}/{task.total:,.0f}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        copy_task = progress.add_task("Copying files", total=total_files)
        copy_jobs: list[tuple[int, MediaFile, Path]] = []
        for group_index, (group, episode_dir) in enumerate(
            zip(groups, episode_directories)
        ):
            if not dry_run:
                episode_dir.mkdir(parents=True, exist_ok=False)

            destinations = plan_group_destinations(group.files, episode_dir)
            for file, destination in zip(group.files, destinations):
                copy_jobs.append((group_index, file, destination))

        if dry_run:
            for group_index, file, destination in copy_jobs:
                episode_groups[group_index].files.append(
                    MediaFile(
                        source=file.source,
                        path=destination,
                        created_at=file.created_at,
                    )
                )
                progress.advance(copy_task)
            return episode_groups

        max_workers = min(max(total_files, 1), 8)
        future_map = {}
        copy_failures: list[str] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for group_index, file, destination in copy_jobs:
                future = executor.submit(
                    copy_file_to_destination, file.path, destination
                )
                future_map[future] = (group_index, file)

            for future in as_completed(future_map):
                group_index, file = future_map[future]
                try:
                    moved_path = future.result()
                    episode_groups[group_index].files.append(
                        MediaFile(
                            source=file.source,
                            path=moved_path,
                            created_at=file.created_at,
                        )
                    )
                except Exception as exc:
                    copy_failures.append(f"{file.path}: {exc}")
                progress.advance(copy_task)

        if copy_failures:
            raise RuntimeError("Failed copies:\n" + "\n".join(copy_failures))

        for episode_group in episode_groups:
            episode_group.files.sort(key=lambda media: media.created_at)

    return episode_groups


def delete_original_media_files(paths: list[Path]) -> tuple[int, list[str]]:
    unique_paths = sorted(set(paths), key=lambda item: str(item))
    if not unique_paths:
        return 0, []

    failures: list[str] = []
    deleted_count = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold red]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed:,.0f}/{task.total:,.0f}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        delete_task = progress.add_task(
            "Deleting original source files", total=len(unique_paths)
        )
        for path in unique_paths:
            try:
                path.unlink()
                deleted_count += 1
            except FileNotFoundError:
                pass
            except Exception as exc:
                failures.append(f"{path}: {exc}")
            progress.advance(delete_task)

    return deleted_count, failures


def ffmpeg_command(input_path: Path, output_path: Path) -> list[str]:
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "debug",
        "-stats",
        "-stats_period",
        "1",
        "-progress",
        "pipe:2",
        "-i",
        str(input_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        "format=yuv420p10le",
        "-c:v",
        "hevc_videotoolbox",
        "-profile:v",
        "main10",
        "-tag:v",
        "hvc1",
        "-b:v",
        "25M",
        "-maxrate",
        "40M",
        "-bufsize",
        "60M",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]


def stream_ffmpeg(input_path: Path, output_path: Path) -> int:
    command = ffmpeg_command(input_path, output_path)
    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    assert process.stderr is not None
    for line in process.stderr:
        console.print(line.rstrip(), markup=False)

    return process.wait()


def convert_mov_files(episode_groups: list[EpisodeGroup], dry_run: bool = False) -> int:
    mov_files: list[Path] = []
    for group in episode_groups:
        for file in group.files:
            if file.path.suffix.lower() == ".mov":
                mov_files.append(file.path)

    if not mov_files:
        console.print("No .mov files found for conversion.")
        return 0

    if shutil.which("ffmpeg") is None:
        console.print("ffmpeg is not available on PATH.", style="bold red")
        return 1

    failures: list[Path] = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold magenta]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed:,.0f}/{task.total:,.0f}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        convert_task = progress.add_task(
            "Converting .mov to _HQ.mp4", total=len(mov_files)
        )

        for mov_file in mov_files:
            output_path = mov_file.with_name(f"{mov_file.stem}_HQ.mp4")
            progress.console.print(
                f"\nConverting: {mov_file} -> {output_path}", style="bold"
            )

            if dry_run:
                progress.advance(convert_task)
                continue

            if output_path.exists():
                progress.console.print(f"Skipping existing output: {output_path}")
                progress.advance(convert_task)
                continue

            code = stream_ffmpeg(mov_file, output_path)
            if code != 0:
                failures.append(mov_file)
            progress.advance(convert_task)

    if failures:
        console.print(f"{len(failures)} conversion(s) failed.", style="bold red")
        for failed in failures:
            console.print(f"  - {failed}", markup=False)
        return 1

    return 0
