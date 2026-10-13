from unittest.mock import MagicMock, Mock

import pytest

from podcast.frameio import resolve_frameio_url


@pytest.fixture
def session(monkeypatch):
    session = MagicMock()
    session.__enter__.return_value = session
    session.get.return_value = Mock(
        url="https://next.frame.io/share/share-id/view/asset-id"
    )
    monkeypatch.setattr("podcast.frameio.requests.Session", Mock(return_value=session))
    return session


def test_frameio_prefers_audio_capable_manifest(session):
    """Resolve the original share into HLS without downloading the huge camera original."""
    session.post.return_value.json.return_value = {
        "data": {
            "share": {"downloadingEnabled": True},
            "asset": {"media": {"hlsManifest": "https://example.com/audio.m3u8"}},
        }
    }
    assert (
        resolve_frameio_url("https://f.io/example") == "https://example.com/audio.m3u8"
    )
    assert session.post.call_args.kwargs["json"]["variables"] == {
        "shareId": "share-id",
        "assetId": "asset-id",
    }


def test_frameio_respects_disabled_downloads(session):
    """A viewable stream must not bypass a share owner's disabled download setting."""
    session.post.return_value.json.return_value = {
        "data": {
            "share": {"downloadingEnabled": False},
            "asset": {"media": {"hlsManifest": "https://example.com/audio.m3u8"}},
        }
    }
    with pytest.raises(ValueError, match="disabled"):
        resolve_frameio_url("https://f.io/example")


def test_frameio_reports_expired_or_protected_share(session):
    """GraphQL access errors need an actionable failure instead of a missing-key crash."""
    session.post.return_value.json.return_value = {
        "errors": [{"message": "not authorized"}]
    }
    with pytest.raises(ValueError, match="authorize"):
        resolve_frameio_url("https://f.io/example")


def test_frameio_uses_smallest_audio_capable_proxy(session):
    """The tiny thumbnail-scrubbing video has no audio, so prefer a real playback proxy."""
    session.post.return_value.json.return_value = {
        "data": {
            "share": {"downloadingEnabled": True},
            "asset": {
                "media": {
                    "videoTranscodes": [
                        {
                            "key": "video_h264_180",
                            "encodeStatus": "SUCCESS",
                            "downloadUrl": "https://example.com/scrub",
                            "filesizeInBytes": 1,
                        },
                        {
                            "key": "h264_720",
                            "encodeStatus": "SUCCESS",
                            "downloadUrl": "https://example.com/large",
                            "filesizeInBytes": 100,
                        },
                        {
                            "key": "h264_360",
                            "encodeStatus": "SUCCESS",
                            "downloadUrl": "https://example.com/small",
                            "filesizeInBytes": 50,
                        },
                    ]
                }
            },
        }
    }
    assert resolve_frameio_url("https://f.io/example") == "https://example.com/small"
