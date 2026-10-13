import sys
from unittest.mock import Mock

from click.testing import CliRunner

from podcast.cli import main
from podcast.transcribe import Segment, Transcript, transcribe


def test_existing_output_is_preserved(tmp_path, monkeypatch):
    """Refuse reruns before transcribing so reviewed briefs cannot be clobbered."""
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    monkeypatch.setattr("podcast.cli.shutil.which", lambda _: "/usr/bin/ffmpeg")
    video = tmp_path / "video.mp4"
    video.write_bytes(b"video")
    output = tmp_path / "summary.md"
    output.write_text("keep me")
    result = CliRunner().invoke(main, [str(video), "-o", str(tmp_path)])
    assert result.exit_code == 1
    assert "already exist" in result.output
    assert output.read_text() == "keep me"


def test_api_failure_preserves_transcript(tmp_path, monkeypatch):
    """An API failure must leave the expensive local transcription available."""
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    monkeypatch.setattr("podcast.cli.shutil.which", lambda _: "/usr/bin/ffmpeg")
    transcript = Transcript(
        language="en", segments=[Segment(start=0, end=5, text="Hello world.")]
    )
    monkeypatch.setattr("podcast.cli.transcribe", Mock(return_value=transcript))
    monkeypatch.setattr(
        "podcast.summary.summarize", Mock(side_effect=RuntimeError("API unavailable"))
    )
    video = tmp_path / "video.mp4"
    video.touch()
    result = CliRunner().invoke(main, [str(video), "-o", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert "API unavailable" in result.output
    assert (
        Transcript.model_validate_json((tmp_path / "out/transcript.json").read_text())
        == transcript
    )
    assert not (tmp_path / "out/summary.md").exists()


def test_silent_audio_does_not_create_a_summary(tmp_path, monkeypatch):
    """Empty speech must fail explicitly rather than invite an invented episode."""
    import pytest

    monkeypatch.setattr("podcast.transcribe.diarizer_binary", Mock())
    monkeypatch.setitem(
        sys.modules,
        "mlx_whisper",
        Mock(
            transcribe=Mock(
                return_value={
                    "language": "en",
                    "segments": [{"start": 0, "end": 1, "text": " "}],
                }
            )
        ),
    )
    monkeypatch.setitem(
        sys.modules, "mlx_whisper.audio", Mock(load_audio=Mock(return_value=[]))
    )
    with pytest.raises(ValueError, match="no speech"):
        transcribe(tmp_path / "silence.wav")
