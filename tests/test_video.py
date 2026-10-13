import wave
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from unittest.mock import MagicMock, Mock

import pytest

from podcast.video import LocalVideo, RemoteVideo, VideoSource


def test_local_file_is_not_copied_or_deleted(tmp_path):
    """Local media belongs to the user and must survive source cleanup."""
    path = tmp_path / "episode with spaces.mp4"
    path.write_bytes(b"video")
    source = VideoSource.from_input(str(path))
    assert isinstance(source, LocalVideo)
    with source.local_path() as local:
        assert local == path
    assert path.read_bytes() == b"video"


@pytest.mark.parametrize(
    "value", ["missing.mp4", "ftp://example.com/video.mp4", "https://"]
)
def test_bad_source_is_rejected(value):
    """Fail before expensive processing when the input cannot be resolved."""
    with pytest.raises(ValueError):
        VideoSource.from_input(value)


def test_playlist_is_rejected_before_download(tmp_path, monkeypatch):
    """A playlist URL must not accidentally download multiple episodes."""
    downloader = MagicMock()
    downloader.__enter__.return_value = downloader
    downloader.extract_info.return_value = {"_type": "playlist"}
    monkeypatch.setattr("podcast.video.YoutubeDL", Mock(return_value=downloader))
    with pytest.raises(ValueError, match="playlist"):
        RemoteVideo("https://example.com/playlist", tmp_path).download_audio(
            tmp_path / "audio.wav"
        )
    downloader.extract_info.assert_called_once_with(
        "https://example.com/playlist", download=False
    )


def test_remote_download_and_cache(tmp_path):
    """An actual HTTP download becomes playable audio and survives later processing failures."""
    with wave.open(str(tmp_path / "episode.wav"), "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(16000)
        audio.writeframes(b"\0\0" * 16000)
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(tmp_path))
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        source = RemoteVideo(
            f"http://127.0.0.1:{server.server_port}/episode.wav", tmp_path / "cache"
        )
        assert isinstance(source, RemoteVideo)
        with pytest.raises(RuntimeError, match="processing failed"):
            with source.local_path() as path:
                with wave.open(str(path)) as audio:
                    assert audio.getnframes() == 16000
                raise RuntimeError("processing failed")
        assert path.exists()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    with source.local_path() as cached:
        assert cached == path
