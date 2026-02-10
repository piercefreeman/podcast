from __future__ import annotations

import os
import shutil
import subprocess
from collections import defaultdict, deque
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

from .config import EPISODE_PATTERN

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


def discover_sources(audiohijack_path: Path, volumes_root: Path = Path("/Volumes")) -> list[SourceOption]:
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


def group_files_by_start_time(files: list[MediaFile], window: timedelta) -> list[MediaGroup]:
    grouped_by_source: dict[str, deque[MediaFile]] = defaultdict(deque)
    for file in sorted(files, key=lambda item: item.created_at):
        grouped_by_source[file.source].append(file)

    groups: list[MediaGroup] = []
    window_seconds = window.total_seconds()

    while True:
        next_candidates = [queue[0] for queue in grouped_by_source.values() if queue]
        if not next_candidates:
            break

        anchor = min(next_candidates, key=lambda item: item.created_at)
        anchor_time = anchor.created_at
        group_files: list[MediaFile] = []

        for queue in grouped_by_source.values():
            if not queue:
                continue
            candidate = queue[0]
            delta = abs((candidate.created_at - anchor_time).total_seconds())
            if delta <= window_seconds:
                group_files.append(queue.popleft())

        if not group_files:
            queue = grouped_by_source[anchor.source]
            if queue:
                group_files.append(queue.popleft())

        groups.append(
            MediaGroup(
                start_time=min(file.created_at for file in group_files),
                files=sorted(group_files, key=lambda item: item.created_at),
            )
        )

    return sorted(groups, key=lambda group: group.start_time)


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


def move_file_to_destination(source_path: Path, destination: Path) -> Path:
    return Path(shutil.move(str(source_path), str(destination)))


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
        move_task = progress.add_task("Moving files", total=total_files)
        move_jobs: list[tuple[int, MediaFile, Path]] = []
        for group_index, (group, episode_dir) in enumerate(zip(groups, episode_directories)):
            if not dry_run:
                episode_dir.mkdir(parents=True, exist_ok=False)

            destinations = plan_group_destinations(group.files, episode_dir)
            for file, destination in zip(group.files, destinations):
                move_jobs.append((group_index, file, destination))

        if dry_run:
            for group_index, file, destination in move_jobs:
                episode_groups[group_index].files.append(
                    MediaFile(
                        source=file.source,
                        path=destination,
                        created_at=file.created_at,
                    )
                )
                progress.advance(move_task)
            return episode_groups

        max_workers = min(max(total_files, 1), 8)
        future_map = {}
        move_failures: list[str] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for group_index, file, destination in move_jobs:
                future = executor.submit(move_file_to_destination, file.path, destination)
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
                    move_failures.append(f"{file.path}: {exc}")
                progress.advance(move_task)

        if move_failures:
            raise RuntimeError("Failed moves:\n" + "\n".join(move_failures))

        for episode_group in episode_groups:
            episode_group.files.sort(key=lambda media: media.created_at)

    return episode_groups


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
        convert_task = progress.add_task("Converting .mov to _HQ.mp4", total=len(mov_files))

        for mov_file in mov_files:
            output_path = mov_file.with_name(f"{mov_file.stem}_HQ.mp4")
            progress.console.print(f"\nConverting: {mov_file} -> {output_path}", style="bold")

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
