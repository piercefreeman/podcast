from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import click
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table

from .config import (
    DEFAULT_AUDIOHIJACK_PATH,
    DEFAULT_PODCAST_ROOT,
    DEFAULT_START_WINDOW,
    FRAMEIO_VAULT_ENTRY,
    FRAMEIO_VAULT_NAME,
    load_frameio_settings,
    parse_duration,
)
from .frame import upload_episode_files_to_frameio
from .resource import (
    EpisodeGroup,
    MediaGroup,
    SourceOption,
    convert_mov_files,
    discover_sources,
    find_existing_episode_numbers,
    group_files_by_start_time,
    move_groups_to_episodes,
    scan_sources_for_today_files,
)

console = Console()


def parse_toggle_indices(raw: str, max_index: int) -> set[int]:
    indices: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        value = int(token)
        if value < 1 or value > max_index:
            raise ValueError(f"Index {value} is out of range 1..{max_index}.")
        indices.add(value)
    return indices


def prompt_for_sources(options: list[SourceOption]) -> list[SourceOption]:
    while True:
        console.print(
            "\nSelectable sources (toggle with comma-separated indices):", style="bold"
        )
        for index, option in enumerate(options, start=1):
            checked = "x" if option.selected else " "
            availability = "" if option.path.exists() else " (missing path)"
            console.print(
                f"{index:>2}. [{checked}] {option.name}: {option.path}{availability}",
                markup=False,
            )

        raw = Prompt.ask(
            "Toggle sources, or press Enter to continue with current selection",
            default="",
        ).strip()
        if not raw:
            selected = [option for option in options if option.selected]
            if selected:
                return selected
            console.print(
                "Select at least one source before continuing.", style="bold red"
            )
            continue

        try:
            indices = parse_toggle_indices(raw, len(options))
        except ValueError as exc:
            console.print(str(exc), style="bold red")
            continue

        for index in indices:
            options[index - 1].selected = not options[index - 1].selected


def normalize_selected_sources(sources: list[SourceOption]) -> list[SourceOption]:
    valid_sources: list[SourceOption] = []
    for source in sources:
        if source.path.exists():
            valid_sources.append(source)
        else:
            console.print(
                f"Skipping missing source path: {source.path}", style="yellow"
            )
    return valid_sources


def print_group_table(groups: list[MediaGroup]) -> None:
    table = Table(title="Aligned Groups")
    table.add_column("#", justify="right")
    table.add_column("Start Time")
    table.add_column("File Count", justify="right")
    table.add_column("Sources")

    for index, group in enumerate(groups, start=1):
        sources = ", ".join(sorted({file.source for file in group.files}))
        table.add_row(
            str(index),
            group.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            str(len(group.files)),
            sources,
        )

    console.print(table)


def parse_start_window(_: click.Context, __: click.Parameter, value: str) -> timedelta:
    try:
        return parse_duration(value)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc


def run(
    start_window: timedelta,
    podcast_root: Path,
    audiohijack_path: Path,
    all_volumes: bool,
    yes: bool,
    dry_run: bool,
    skip_frameio_upload: bool,
) -> int:
    try:
        frameio_settings = load_frameio_settings()
    except RuntimeError as exc:
        console.print(str(exc), style="bold red", markup=False)
        return 1

    console.print(
        "Frame.io settings loaded from settings/vault "
        f"({FRAMEIO_VAULT_NAME}/{FRAMEIO_VAULT_ENTRY}).",
        style="green",
    )

    source_options = discover_sources(audiohijack_path)
    if all_volumes:
        for option in source_options:
            option.selected = True

    if yes or not sys.stdin.isatty():
        selected_sources = [option for option in source_options if option.selected]
    else:
        selected_sources = prompt_for_sources(source_options)

    selected_sources = normalize_selected_sources(selected_sources)
    if not selected_sources:
        console.print("No valid source paths selected.", style="bold red")
        return 1

    now = datetime.now()
    day_start = datetime(now.year, now.month, now.day)
    day_end = day_start + timedelta(days=1)
    console.print(f"Finding files created on {day_start:%Y-%m-%d}...")

    scanned_files = scan_sources_for_today_files(selected_sources, day_start, day_end)
    if not scanned_files:
        console.print("No files created today were found in selected sources.")
        return 0

    groups = group_files_by_start_time(scanned_files, start_window)
    if not groups:
        console.print("No aligned groups were generated.")
        return 0

    print_group_table(groups)

    existing_episodes = find_existing_episode_numbers(podcast_root)
    next_episode = (existing_episodes[-1] + 1) if existing_episodes else 1
    console.print(
        f"Detected {len(existing_episodes)} existing episode_* folder(s) in {podcast_root}. "
        f"Next is episode_{next_episode}."
    )

    if not yes and not dry_run:
        proceed = Confirm.ask(
            "Continue with moving files and converting .mov files?",
            default=True,
        )
        if not proceed:
            console.print("Aborted.")
            return 0

    try:
        episode_groups: list[EpisodeGroup] = move_groups_to_episodes(
            groups,
            podcast_root,
            dry_run=dry_run,
        )
    except RuntimeError as exc:
        console.print(str(exc), style="bold red", markup=False)
        return 1

    if dry_run:
        console.print("Dry run complete. No files were moved or converted.")
        return 0

    convert_code = convert_mov_files(episode_groups, dry_run=False)
    if convert_code != 0:
        return convert_code

    if skip_frameio_upload:
        console.print("Skipping Frame.io upload (--skip-frameio-upload).")
        return 0

    return upload_episode_files_to_frameio(
        episode_groups,
        token=frameio_settings.token.get_secret_value(),
        destination_id=frameio_settings.destination_id,
    )


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Group today's media files by start time and move them into episode folders.",
)
@click.option(
    "--start-window",
    default=DEFAULT_START_WINDOW,
    show_default=True,
    callback=parse_start_window,
    help="Allowed timestamp delta for grouping (examples: 5min, 90s, 1h).",
)
@click.option(
    "--podcast-root",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=DEFAULT_PODCAST_ROOT,
    show_default=True,
    help="Root directory that contains episode_* folders.",
)
@click.option(
    "--audiohijack-path",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=DEFAULT_AUDIOHIJACK_PATH,
    show_default=True,
    help="Hard-coded AudioHijack source path.",
)
@click.option(
    "--all-volumes",
    is_flag=True,
    help="Preselect every mounted volume in addition to AudioHijack.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip interactive prompts and continue with selected defaults.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show planned actions but do not move files, run ffmpeg, or upload to Frame.io.",
)
@click.option(
    "--skip-frameio-upload",
    is_flag=True,
    help="Skip uploading audio and .mp4 files to Frame.io after conversion.",
)
def cli(
    start_window: timedelta,
    podcast_root: Path,
    audiohijack_path: Path,
    all_volumes: bool,
    yes: bool,
    dry_run: bool,
    skip_frameio_upload: bool,
) -> int:
    return run(
        start_window=start_window,
        podcast_root=podcast_root,
        audiohijack_path=audiohijack_path,
        all_volumes=all_volumes,
        yes=yes,
        dry_run=dry_run,
        skip_frameio_upload=skip_frameio_upload,
    )


def main() -> int:
    return cli(standalone_mode=False)


if __name__ == "__main__":
    raise SystemExit(main())
