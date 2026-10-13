import fcntl
import hashlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import AwareDatetime, BaseModel, TypeAdapter

CACHE_TTL = timedelta(days=7)


class CacheEntry(BaseModel):
    path: str
    expires_at: AwareDatetime


INDEX = TypeAdapter(dict[str, CacheEntry])


def cached_audio(url: str, directory: Path, download: Callable[[Path], None]) -> Path:
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / f"{hashlib.sha256(url.encode()).hexdigest()}.wav"
    index_path = directory / "index.json"
    # Hold the lock through download so concurrent runs share a single cached file.
    with (directory / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        index = (
            INDEX.validate_json(index_path.read_text()) if index_path.exists() else {}
        )
        entry = index.get(url)
        if (
            entry
            and entry.expires_at > datetime.now(UTC)
            and entry.path == path.name
            and path.is_file()
            and path.stat().st_size > 0
        ):
            return path
        with TemporaryDirectory(prefix="download-", dir=directory) as temporary:
            audio = Path(temporary) / "audio.wav"
            download(audio)
            if not audio.is_file() or audio.stat().st_size == 0:
                raise ValueError(
                    "Download produced no audio; the cache was not updated."
                )
            audio.replace(path)
        index[url] = CacheEntry(
            path=path.name, expires_at=datetime.now(UTC) + CACHE_TTL
        )
        pending = directory / ".index.tmp"
        pending.write_bytes(INDEX.dump_json(index, indent=2) + b"\n")
        pending.replace(index_path)
        return path
