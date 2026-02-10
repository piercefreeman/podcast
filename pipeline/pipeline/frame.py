from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from .config import (
    AUDIO_EXTENSIONS,
    VIDEO_UPLOAD_EXTENSIONS,
)
from .resource import EpisodeGroup

console = Console()


@dataclass(frozen=True)
class UploadJob:
    episode_dir: Path
    file_path: Path
    destination_folder_id: str


def collect_upload_candidates(episode_groups: list[EpisodeGroup]) -> dict[Path, list[Path]]:
    candidates: dict[Path, list[Path]] = {}
    for group in episode_groups:
        files: list[Path] = []
        for path in sorted(group.episode_dir.iterdir(), key=lambda item: item.name.lower()):
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            if suffix in AUDIO_EXTENSIONS or suffix in VIDEO_UPLOAD_EXTENSIONS:
                files.append(path)
        if files:
            candidates[group.episode_dir] = files
    return candidates


def resolve_frameio_destination_folder_id(client: Any, destination_id: str) -> str:
    try:
        destination_asset = client.assets.get(destination_id)
        destination_type = destination_asset.get("type")
        if destination_type == "project":
            return destination_asset["root_asset_id"]
        return destination_asset["id"]
    except Exception:
        project = client.projects.get(destination_id)
        return project["root_asset_id"]


def iter_asset_children(client: Any, parent_asset_id: str) -> list[dict[str, Any]]:
    children = client.assets.get_children(parent_asset_id)
    if isinstance(children, list):
        return children
    return list(children)


def ensure_remote_episode_folder(client: Any, parent_asset_id: str, episode_name: str) -> str:
    for child in iter_asset_children(client, parent_asset_id):
        if child.get("type") == "folder" and child.get("name") == episode_name:
            return child["id"]

    created = client.assets.create(
        parent_asset_id,
        type="folder",
        name=episode_name,
    )
    return created["id"]


def upload_file_to_frameio(client: Any, destination_folder_id: str, local_file: Path) -> dict[str, Any]:
    file_info = client.assets.build_asset_info(str(local_file))
    mimetype = file_info["mimetype"] or "application/octet-stream"

    remote_asset = client.assets.create(
        destination_folder_id,
        type="file",
        name=local_file.name,
        filetype=mimetype,
        filesize=file_info["filesize"],
    )

    with local_file.open("rb") as handle:
        client.assets._upload(remote_asset, handle)

    return remote_asset


def upload_episode_files_to_frameio(
    episode_groups: list[EpisodeGroup],
    token: str,
    destination_id: str,
) -> int:
    upload_candidates = collect_upload_candidates(episode_groups)
    if not upload_candidates:
        console.print("No audio or .mp4 files found for Frame.io upload.")
        return 0

    try:
        from frameioclient import FrameioClient
    except ImportError as exc:
        console.print(f"Failed to import frameioclient: {exc}", style="bold red", markup=False)
        return 1

    try:
        client = FrameioClient(token)
    except Exception as exc:
        console.print(
            "Failed to initialize Frame.io client. "
            "Ensure urllib3<2 is installed and FRAMEIO_TOKEN is valid.",
            style="bold red",
        )
        console.print(str(exc), style="red", markup=False)
        return 1

    try:
        destination_root_id = resolve_frameio_destination_folder_id(client, destination_id)
    except Exception as exc:
        console.print(
            f"Failed to resolve FRAMEIO_DESTINATION_ID '{destination_id}': {exc}",
            style="bold red",
            markup=False,
        )
        return 1

    upload_jobs: list[UploadJob] = []
    for episode_dir in sorted(upload_candidates, key=lambda item: item.name.lower()):
        try:
            remote_folder_id = ensure_remote_episode_folder(client, destination_root_id, episode_dir.name)
        except Exception as exc:
            console.print(
                f"Failed to create/find remote folder '{episode_dir.name}': {exc}",
                style="bold red",
                markup=False,
            )
            return 1

        for file_path in upload_candidates[episode_dir]:
            upload_jobs.append(
                UploadJob(
                    episode_dir=episode_dir,
                    file_path=file_path,
                    destination_folder_id=remote_folder_id,
                )
            )

    failures: list[str] = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed:,.0f}/{task.total:,.0f}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        upload_task = progress.add_task("Uploading to Frame.io", total=len(upload_jobs))
        for job in upload_jobs:
            progress.console.print(
                f"Uploading {job.file_path.name} -> {job.episode_dir.name}",
                markup=False,
            )
            try:
                upload_file_to_frameio(
                    client,
                    destination_folder_id=job.destination_folder_id,
                    local_file=job.file_path,
                )
            except Exception as exc:
                failures.append(f"{job.file_path}: {exc}")
            progress.advance(upload_task)

    if failures:
        console.print(f"{len(failures)} upload(s) failed.", style="bold red")
        for failure in failures:
            console.print(f"  - {failure}", markup=False)
        return 1

    return 0
