import subprocess
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from yt_dlp import YoutubeDL

from .cache import cached_audio
from .frameio import resolve_frameio_url


class VideoSource(ABC):
    @staticmethod
    def from_input(value: str) -> "VideoSource":
        url = urlsplit(value)
        if url.scheme in {"http", "https"} and url.hostname:
            return RemoteVideo(value)
        if url.scheme:
            raise ValueError("Use a local path or an HTTP(S) video URL.")
        path = Path(value).expanduser().resolve()
        if not path.is_file():
            raise ValueError(f"Video does not exist: {path}")
        return LocalVideo(path)

    @abstractmethod
    def local_path(self) -> AbstractContextManager[Path]:
        """Provide local media suitable for transcription."""


@dataclass(frozen=True)
class LocalVideo(VideoSource):
    path: Path

    @contextmanager
    def local_path(self) -> Iterator[Path]:
        yield self.path


@dataclass(frozen=True)
class RemoteVideo(VideoSource):
    url: str
    cache_dir: Path = Path(".cache/audio")

    @contextmanager
    def local_path(self) -> Iterator[Path]:
        yield cached_audio(self.url, self.cache_dir, self.download_audio)

    def download_audio(self, destination: Path) -> None:
        url = self.url
        if urlsplit(url).hostname in {"f.io", "next.frame.io"}:
            url = resolve_frameio_url(url)
        with YoutubeDL(
            {
                "outtmpl": str(destination.parent / "source.%(ext)s"),
                "format": "bestaudio/best",
                "noplaylist": True,
                "quiet": True,
                "noprogress": True,
                "socket_timeout": 30,
                "concurrent_fragment_downloads": 8,
                "skip_unavailable_fragments": False,
            }
        ) as downloader:
            info = downloader.extract_info(url, download=False)
            if not info or info.get("_type") in {"playlist", "multi_video"}:
                raise ValueError("Provide one video URL, not a playlist.")
            info = downloader.extract_info(url, download=True)
            path = Path(downloader.prepare_filename(info))
            extract_audio(str(path), destination)


def extract_audio(source: str, destination: Path) -> None:
    result = subprocess.run(
        [
            "ffmpeg",
            "-nostdin",
            "-v",
            "error",
            "-i",
            source,
            "-map",
            "0:a:0",
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-c:a",
            "pcm_s16le",
            str(destination),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise ValueError(f"Could not extract audio: {result.stderr.strip()}")
