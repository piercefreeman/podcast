from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

from pydantic import SecretStr, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from vaultdantic import OnePasswordConfigDict, VaultMixin

DEFAULT_START_WINDOW = "5min"
DEFAULT_PODCAST_ROOT = Path("/Volumes/Common_Drive/podcast")
DEFAULT_AUDIOHIJACK_PATH = Path("/Users/piercefreeman/Music/Audio Hijack")

EPISODE_PATTERN = re.compile(r"^episode_(\d+)$")
WINDOW_PATTERN = re.compile(
    r"^\s*(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours)\s*$",
    re.IGNORECASE,
)

AUDIO_EXTENSIONS = {
    ".aac",
    ".aif",
    ".aiff",
    ".alac",
    ".flac",
    ".m4a",
    ".mp3",
    ".ogg",
    ".opus",
    ".wav",
    ".wma",
}
VIDEO_UPLOAD_EXTENSIONS = {".mp4"}

FRAMEIO_VAULT_NAME = "Side-Projects"
FRAMEIO_VAULT_ENTRY = "Pretrained-Pipeline"


class FrameioSettings(BaseSettings, VaultMixin):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="FRAMEIO_",
        extra="ignore",
    )
    model_vault_config = OnePasswordConfigDict(
        vault=FRAMEIO_VAULT_NAME,
        entry=FRAMEIO_VAULT_ENTRY,
    )

    token: SecretStr
    destination_name: str

    @field_validator("destination_name")
    @classmethod
    def validate_destination_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("FRAMEIO_DESTINATION_NAME cannot be empty.")
        return normalized


def load_frameio_settings() -> FrameioSettings:
    try:
        return FrameioSettings()
    except ValidationError as exc:
        missing_keys = []
        for error in exc.errors():
            location = error.get("loc", [])
            if location:
                field_name = location[0]
                if field_name == "token":
                    missing_keys.append("FRAMEIO_TOKEN")
                if field_name == "destination_name":
                    missing_keys.append("FRAMEIO_DESTINATION_NAME")
        if missing_keys:
            missing = ", ".join(sorted(set(missing_keys)))
            raise RuntimeError(
                "Missing Frame.io settings in .env/vault. Required keys: "
                f"{missing}. See .env.example."
            ) from exc
        raise RuntimeError(f"Invalid Frame.io settings: {exc}") from exc


def parse_duration(window: str) -> timedelta:
    match = WINDOW_PATTERN.match(window)
    if not match:
        raise ValueError(
            f"Invalid --start-window value '{window}'. Use forms like 5min, 30s, or 1h."
        )

    amount = int(match.group(1))
    unit = match.group(2).lower()
    if unit.startswith("s"):
        return timedelta(seconds=amount)
    if unit.startswith("m"):
        return timedelta(minutes=amount)
    return timedelta(hours=amount)
