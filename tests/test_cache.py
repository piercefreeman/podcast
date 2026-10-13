import json
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from podcast.cache import cached_audio


def test_cache_hit_keeps_audio_and_original_url(tmp_path):
    """The original URL should resolve locally until its cache entry expires."""
    url = "https://f.io/example"
    download = Mock(side_effect=lambda path: path.write_bytes(b"audio"))
    first = cached_audio(url, tmp_path, download)
    second = cached_audio(url, tmp_path, download)
    assert first == second
    assert first.read_bytes() == b"audio"
    download.assert_called_once()
    entry = json.loads((tmp_path / "index.json").read_text())[url]
    assert entry["path"] == first.name
    assert datetime.fromisoformat(entry["expires_at"]) > datetime.now(UTC)


@pytest.mark.parametrize("stale", ["expired", "missing", "empty"])
def test_stale_audio_is_refreshed(tmp_path, stale):
    """Expired, deleted, or empty cached media must be downloaded again."""
    download = Mock(side_effect=lambda path: path.write_bytes(b"audio"))
    url = "https://example.com/video.mp4"
    path = cached_audio(url, tmp_path, download)
    if stale == "expired":
        index_path = tmp_path / "index.json"
        index = json.loads(index_path.read_text())
        index[url]["expires_at"] = (
            datetime.now(UTC) - timedelta(seconds=1)
        ).isoformat()
        index_path.write_text(json.dumps(index))
    elif stale == "missing":
        path.unlink()
    else:
        path.write_bytes(b"")
    assert cached_audio(url, tmp_path, download).read_bytes() == b"audio"
    assert download.call_count == 2


def test_partial_download_is_not_indexed(tmp_path):
    """Interrupted downloads must never become cache hits."""

    def fail(path):
        path.write_bytes(b"partial")
        raise RuntimeError("interrupted")

    with pytest.raises(RuntimeError, match="interrupted"):
        cached_audio("https://example.com/video.mp4", tmp_path, fail)
    assert not (tmp_path / "index.json").exists()
    assert not list(tmp_path.glob("*.wav"))
    assert not list(tmp_path.glob("download-*"))
