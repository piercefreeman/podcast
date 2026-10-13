import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from podcast.diarize import SpeakerTurn, diarize, diarizer_binary


@pytest.mark.parametrize("count", [None, 2, 3])
def test_coreml_command_preserves_short_turns_and_cleans_temporary_files(
    tmp_path, monkeypatch, count
):
    monkeypatch.setattr("podcast.diarize.diarizer_binary", lambda: tmp_path / "runner")
    monkeypatch.setattr(
        "podcast.diarize.extract_audio", lambda source, dest: dest.write_bytes(b"audio")
    )
    outputs = []

    def run(command, **kwargs):
        assert command[command.index("--compute-units") + 1] == "ane"
        assert command[command.index("--mode") + 1] == "offline"
        if count is None:
            assert command[-4:] == ["--min-speakers", "2", "--max-speakers", "3"]
        else:
            assert command[-2:] == ["--num-speakers", str(count)]
        output = Path(command[command.index("--output") + 1])
        outputs.append(output)
        output.write_text(
            json.dumps(
                {
                    "segments": [
                        {
                            "startTimeSeconds": 1,
                            "endTimeSeconds": 1.3,
                            "speakerId": "S9",
                        },
                        {"startTimeSeconds": 0, "endTimeSeconds": 1, "speakerId": "S4"},
                        {"startTimeSeconds": 2, "endTimeSeconds": 3, "speakerId": "S4"},
                    ]
                }
            )
        )
        return Mock(returncode=0)

    monkeypatch.setattr("podcast.diarize.subprocess.run", run)
    assert diarize(tmp_path / "video.mp4", count) == [
        SpeakerTurn(0, 1, "speaker_1"),
        SpeakerTurn(1, 1.3, "speaker_2"),
        SpeakerTurn(2, 3, "speaker_1"),
    ]
    assert not outputs[0].parent.exists()


def test_native_failure_is_not_reported_as_a_valid_transcript(tmp_path, monkeypatch):
    monkeypatch.setattr("podcast.diarize.diarizer_binary", lambda: tmp_path / "runner")
    monkeypatch.setattr("podcast.diarize.extract_audio", Mock())
    monkeypatch.setattr(
        "podcast.diarize.subprocess.run",
        Mock(return_value=Mock(returncode=1, stderr="Model failed")),
    )
    with pytest.raises(ValueError, match="Model failed"):
        diarize(tmp_path / "video.mp4")


def test_missing_runner_explains_setup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="make setup-diarizer"):
        diarizer_binary()


@pytest.mark.parametrize(
    "start,end", [(-1, 1), (1, 0), (0, float("nan")), (0, float("inf"))]
)
def test_native_timestamps_must_be_valid(start, end):
    with pytest.raises(ValueError, match="timestamps"):
        SpeakerTurn(start, end, "speaker_1")
