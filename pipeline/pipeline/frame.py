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


@dataclass(frozen=True)
class FrameioUploadContext:
    client: Any
    destination_root_id: str


def collect_upload_candidates(
    episode_groups: list[EpisodeGroup],
) -> dict[Path, list[Path]]:
    candidates: dict[Path, list[Path]] = {}
    for group in episode_groups:
        files: list[Path] = []
        for path in sorted(
            group.episode_dir.iterdir(), key=lambda item: item.name.lower()
        ):
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            if suffix in AUDIO_EXTENSIONS or suffix in VIDEO_UPLOAD_EXTENSIONS:
                files.append(path)
        if files:
            candidates[group.episode_dir] = files
    return candidates


def _as_list(results: Any) -> list[dict[str, Any]]:
    if isinstance(results, list):
        return results
    return list(results)


def _collect_projects_for_name_resolution(client: Any) -> list[tuple[str, str, str]]:
    projects: list[tuple[str, str, str]] = []
    details: list[str] = []
    seen_project_ids: set[str] = set()

    def add_project(project_source: str, project_name: str, project_id: str) -> None:
        if not project_name or not project_id:
            return
        if project_id in seen_project_ids:
            return
        seen_project_ids.add(project_id)
        projects.append((project_source, project_name, project_id))

    try:
        before = len(projects)
        teams = _as_list(client.teams.list_all())
        for team in teams:
            team_id = team.get("id")
            if not team_id:
                continue
            team_name = str(team.get("name", "unknown-team"))
            team_projects = _as_list(client.teams.list_projects(team_id))
            for project in team_projects:
                add_project(
                    project_source=team_name,
                    project_name=str(project.get("name", "")),
                    project_id=str(project.get("id", "")),
                )
        if len(projects) == before:
            details.append("/teams returned 0 projects")
    except Exception as exc:
        details.append(f"/teams lookup failed: {exc}")

    try:
        before = len(projects)
        me = client.users.get_me()
        account_id = me.get("account_id") if isinstance(me, dict) else None
        if account_id:
            account_teams = _as_list(client.teams.list(account_id))
            for team in account_teams:
                team_id = team.get("id")
                if not team_id:
                    continue
                team_name = str(team.get("name", "unknown-team"))
                team_projects = _as_list(client.teams.list_projects(team_id))
                for project in team_projects:
                    add_project(
                        project_source=team_name,
                        project_name=str(project.get("name", "")),
                        project_id=str(project.get("id", "")),
                    )
            if len(projects) == before:
                details.append("/accounts/{account_id}/teams returned 0 projects")
        else:
            details.append("/me lookup returned no account_id")
    except Exception as exc:
        details.append(f"/accounts/{{account_id}}/teams lookup failed: {exc}")

    if projects:
        return projects

    try:
        before = len(projects)
        listed_projects = _as_list(client._api_call("get", "/projects"))
        for project in listed_projects:
            add_project(
                project_source="projects",
                project_name=str(project.get("name", "")),
                project_id=str(project.get("id", "")),
            )
        if len(projects) == before:
            details.append("/projects returned 0 projects")
    except Exception as exc:
        details.append(f"/projects lookup failed: {exc}")

    try:
        before = len(projects)
        shared_projects = _as_list(client._api_call("get", "/projects/shared"))
        for project in shared_projects:
            add_project(
                project_source="projects/shared",
                project_name=str(project.get("name", "")),
                project_id=str(project.get("id", "")),
            )
        if len(projects) == before:
            details.append("/projects/shared returned 0 projects")
    except Exception as exc:
        details.append(f"/projects/shared lookup failed: {exc}")

    if projects:
        return projects

    summary = " | ".join(details) if details else "no project listing endpoints returned data"
    raise RuntimeError(
        "Unable to list Frame.io projects for name resolution "
        f"({summary}). Ensure FRAMEIO_TOKEN can read accessible projects."
    )


def resolve_frameio_destination_folder_id(
    client: Any,
    destination_name: str,
) -> str:
    projects = _collect_projects_for_name_resolution(client)
    exact_matches: list[tuple[str, str, str]] = []
    casefold_matches: list[tuple[str, str, str]] = []
    for project_source, project_name, project_id in projects:
        entry = (project_source, project_name, project_id)
        if project_name == destination_name:
            exact_matches.append(entry)
        if project_name.lower() == destination_name.lower():
            casefold_matches.append(entry)

    matches = exact_matches if exact_matches else casefold_matches
    if not matches:
        raise RuntimeError(
            f"Failed to find Frame.io project named '{destination_name}' for this token."
        )
    if len(matches) > 1:
        options = ", ".join(
            f"{team}/{project} ({project_id})"
            for team, project, project_id in matches[:5]
        )
        raise RuntimeError(
            "Frame.io destination name is ambiguous. "
            f"Multiple projects matched '{destination_name}': {options}. "
            "Use a unique project name."
        )

    _, _, project_id = matches[0]
    project = client.projects.get(project_id)
    return project["root_asset_id"]


def iter_asset_children(client: Any, parent_asset_id: str) -> list[dict[str, Any]]:
    children = client.assets.get_children(parent_asset_id)
    if isinstance(children, list):
        return children
    return list(children)


def ensure_remote_episode_folder(
    client: Any, parent_asset_id: str, episode_name: str
) -> str:
    for child in iter_asset_children(client, parent_asset_id):
        if child.get("type") == "folder" and child.get("name") == episode_name:
            return child["id"]

    created = client.assets.create(
        parent_asset_id,
        type="folder",
        name=episode_name,
    )
    return created["id"]


def upload_file_to_frameio(
    client: Any, destination_folder_id: str, local_file: Path
) -> dict[str, Any]:
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


def build_frameio_upload_context(
    token: str,
    destination_name: str,
) -> FrameioUploadContext:
    try:
        from frameioclient import FrameioClient
    except ImportError as exc:
        raise RuntimeError(f"Failed to import frameioclient: {exc}") from exc

    try:
        client = FrameioClient(token)
    except Exception as exc:
        raise RuntimeError(
            "Failed to initialize Frame.io client. "
            "Ensure urllib3<2 is installed and FRAMEIO_TOKEN is valid. "
            f"Details: {exc}"
        ) from exc

    try:
        destination_root_id = resolve_frameio_destination_folder_id(
            client,
            destination_name=destination_name,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to resolve Frame.io destination name '{destination_name}': {exc}"
        ) from exc

    return FrameioUploadContext(
        client=client,
        destination_root_id=destination_root_id,
    )


def upload_episode_files_to_frameio(
    episode_groups: list[EpisodeGroup],
    token: str,
    destination_name: str,
    context: FrameioUploadContext | None = None,
) -> int:
    upload_candidates = collect_upload_candidates(episode_groups)
    if not upload_candidates:
        console.print("No audio or .mp4 files found for Frame.io upload.")
        return 0

    try:
        upload_context = context or build_frameio_upload_context(
            token,
            destination_name=destination_name,
        )
    except RuntimeError as exc:
        console.print(
            str(exc),
            style="bold red",
            markup=False,
        )
        return 1

    client = upload_context.client
    destination_root_id = upload_context.destination_root_id

    upload_jobs: list[UploadJob] = []
    for episode_dir in sorted(upload_candidates, key=lambda item: item.name.lower()):
        try:
            remote_folder_id = ensure_remote_episode_folder(
                client, destination_root_id, episode_dir.name
            )
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
