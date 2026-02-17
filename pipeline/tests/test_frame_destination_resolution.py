from __future__ import annotations

import unittest
from typing import Any

from pipeline.frame import resolve_frameio_destination_folder_id


class _FakeProjects:
    def __init__(self, projects_by_id: dict[str, dict[str, Any]]) -> None:
        self.projects_by_id = projects_by_id

    def get(self, project_id: str) -> dict[str, Any]:
        if project_id not in self.projects_by_id:
            raise RuntimeError("not found")
        return self.projects_by_id[project_id]


class _FakeTeams:
    def __init__(
        self,
        teams: list[dict[str, Any]],
        projects_by_team: dict[str, list[dict[str, Any]]],
    ) -> None:
        self.teams = teams
        self.projects_by_team = projects_by_team

    def list_all(self) -> list[dict[str, Any]]:
        return self.teams

    def list(self, account_id: str) -> list[dict[str, Any]]:
        return self.teams

    def list_projects(self, team_id: str) -> list[dict[str, Any]]:
        return self.projects_by_team.get(team_id, [])


class _FailingTeams:
    def list_all(self) -> list[dict[str, Any]]:
        raise RuntimeError("unauthorized")

    def list(self, account_id: str) -> list[dict[str, Any]]:
        raise RuntimeError("unauthorized")

    def list_projects(self, team_id: str) -> list[dict[str, Any]]:
        raise RuntimeError("unauthorized")


class _FakeUsers:
    def __init__(self, account_id: str = "account-1", fail_me: bool = False) -> None:
        self.account_id = account_id
        self.fail_me = fail_me

    def get_me(self) -> dict[str, str]:
        if self.fail_me:
            raise RuntimeError("forbidden")
        return {"account_id": self.account_id}


class _FakeClient:
    def __init__(
        self,
        projects_by_id: dict[str, dict[str, Any]],
        teams: list[dict[str, Any]] | None = None,
        projects_by_team: dict[str, list[dict[str, Any]]] | None = None,
        api_projects: list[dict[str, Any]] | None = None,
        api_shared_projects: list[dict[str, Any]] | None = None,
        fail_teams: bool = False,
        fail_projects_api: bool = False,
        fail_shared_projects_api: bool = False,
        fail_me: bool = False,
    ) -> None:
        self.projects = _FakeProjects(projects_by_id)
        self.teams = (
            _FailingTeams()
            if fail_teams
            else _FakeTeams(teams or [], projects_by_team or {})
        )
        self.users = _FakeUsers(fail_me=fail_me)
        self.api_projects = api_projects or []
        self.api_shared_projects = api_shared_projects or []
        self.fail_projects_api = fail_projects_api
        self.fail_shared_projects_api = fail_shared_projects_api

    def _api_call(self, method: str, endpoint: str):
        if method == "get" and endpoint == "/projects":
            if self.fail_projects_api:
                raise RuntimeError("forbidden")
            return self.api_projects
        if method == "get" and endpoint == "/projects/shared":
            if self.fail_shared_projects_api:
                raise RuntimeError("forbidden")
            return self.api_shared_projects
        raise RuntimeError("unsupported")


class FrameDestinationResolutionTests(unittest.TestCase):
    def test_resolve_destination_by_name_exact_match(self) -> None:
        client = _FakeClient(
            projects_by_id={
                "project-1": {"id": "project-1", "root_asset_id": "root-1"}
            },
            teams=[{"id": "team-1", "name": "Team One"}],
            projects_by_team={
                "team-1": [{"id": "project-1", "name": "Podcast Ingest"}]
            },
        )
        resolved = resolve_frameio_destination_folder_id(
            client,
            destination_name="Podcast Ingest",
        )
        self.assertEqual(resolved, "root-1")

    def test_resolve_destination_by_name_casefold_match(self) -> None:
        client = _FakeClient(
            projects_by_id={
                "project-1": {"id": "project-1", "root_asset_id": "root-1"}
            },
            teams=[{"id": "team-1", "name": "Team One"}],
            projects_by_team={
                "team-1": [{"id": "project-1", "name": "podcast ingest"}]
            },
        )
        resolved = resolve_frameio_destination_folder_id(
            client, destination_name="Podcast Ingest"
        )
        self.assertEqual(resolved, "root-1")

    def test_resolve_destination_prefers_exact_name_over_casefold_matches(self) -> None:
        client = _FakeClient(
            projects_by_id={
                "project-1": {"id": "project-1", "root_asset_id": "root-1"},
                "project-2": {"id": "project-2", "root_asset_id": "root-2"},
            },
            teams=[{"id": "team-1", "name": "Team One"}],
            projects_by_team={
                "team-1": [
                    {"id": "project-1", "name": "Podcast Ingest"},
                    {"id": "project-2", "name": "podcast ingest"},
                ]
            },
        )
        resolved = resolve_frameio_destination_folder_id(
            client, destination_name="Podcast Ingest"
        )
        self.assertEqual(resolved, "root-1")

    def test_resolve_destination_by_name_falls_back_to_projects_endpoint(self) -> None:
        client = _FakeClient(
            projects_by_id={
                "project-1": {"id": "project-1", "root_asset_id": "root-1"}
            },
            api_projects=[{"id": "project-1", "name": "Podcast Ingest"}],
            fail_teams=True,
            fail_me=True,
        )
        resolved = resolve_frameio_destination_folder_id(
            client, destination_name="Podcast Ingest"
        )
        self.assertEqual(resolved, "root-1")

    def test_resolve_destination_by_name_falls_back_to_shared_projects_endpoint(self) -> None:
        client = _FakeClient(
            projects_by_id={
                "project-1": {"id": "project-1", "root_asset_id": "root-1"}
            },
            api_shared_projects=[{"id": "project-1", "name": "Podcast Ingest"}],
            fail_teams=True,
            fail_me=True,
            fail_projects_api=True,
        )
        resolved = resolve_frameio_destination_folder_id(
            client, destination_name="Podcast Ingest"
        )
        self.assertEqual(resolved, "root-1")

    def test_resolve_destination_by_name_ambiguous_raises(self) -> None:
        client = _FakeClient(
            projects_by_id={
                "project-1": {"id": "project-1", "root_asset_id": "root-1"},
                "project-2": {"id": "project-2", "root_asset_id": "root-2"},
            },
            teams=[
                {"id": "team-1", "name": "Team One"},
                {"id": "team-2", "name": "Team Two"},
            ],
            projects_by_team={
                "team-1": [{"id": "project-1", "name": "Podcast Ingest"}],
                "team-2": [{"id": "project-2", "name": "Podcast Ingest"}],
            },
        )
        with self.assertRaises(RuntimeError) as exc_info:
            resolve_frameio_destination_folder_id(
                client, destination_name="Podcast Ingest"
            )
        self.assertIn("ambiguous", str(exc_info.exception).lower())

    def test_resolve_destination_by_name_not_found_raises(self) -> None:
        client = _FakeClient(
            projects_by_id={"project-1": {"id": "project-1", "root_asset_id": "root-1"}},
            teams=[{"id": "team-1", "name": "Team One"}],
            projects_by_team={"team-1": [{"id": "project-1", "name": "Other Project"}]},
        )
        with self.assertRaises(RuntimeError) as exc_info:
            resolve_frameio_destination_folder_id(
                client, destination_name="Podcast Ingest"
            )
        self.assertIn("failed to find", str(exc_info.exception).lower())

    def test_resolve_destination_by_name_listing_failures_raise(self) -> None:
        client = _FakeClient(
            projects_by_id={},
            fail_teams=True,
            fail_me=True,
            fail_projects_api=True,
            fail_shared_projects_api=True,
        )
        with self.assertRaises(RuntimeError) as exc_info:
            resolve_frameio_destination_folder_id(
                client, destination_name="Podcast Ingest"
            )
        self.assertIn("unable to list", str(exc_info.exception).lower())


if __name__ == "__main__":
    unittest.main()
