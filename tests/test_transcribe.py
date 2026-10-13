import sys
from unittest.mock import Mock

from podcast.diarize import SpeakerTurn, speaker_at
from podcast.transcribe import label_words, transcribe


def test_whisper_segment_splits_at_each_of_three_speakers():
    raw = [
        {
            "start": 0,
            "end": 3,
            "text": "I'm Alex. I'm Sam. I'm Jo.",
            "words": [
                {"start": 0, "end": 0.4, "word": "I'm"},
                {"start": 0.4, "end": 1, "word": " Alex."},
                {"start": 1, "end": 2, "word": " I'm Sam."},
                {"start": 2, "end": 3, "word": " I'm Jo."},
            ],
        }
    ]
    turns = [SpeakerTurn(i, i + 1, f"speaker_{i + 1}") for i in range(3)]
    segments = label_words(raw, turns)
    assert [(s.speaker, s.start, s.end, s.text) for s in segments] == [
        ("speaker_1", 0, 1, "I'm Alex."),
        ("speaker_2", 1, 2, "I'm Sam."),
        ("speaker_3", 2, 3, "I'm Jo."),
    ]


def test_ambiguous_overlap_and_gaps_stay_unknown():
    turns = [SpeakerTurn(0, 1, "speaker_1"), SpeakerTurn(0, 1, "speaker_2")]
    assert speaker_at(0, 1, turns) is None
    assert speaker_at(2, 3, turns) is None
    assert speaker_at(0, 0, turns) is None
    assert (
        speaker_at(
            0, 1, [SpeakerTurn(0, 1, "speaker_1"), SpeakerTurn(0.2, 1, "speaker_2")]
        )
        is None
    )


def test_word_uses_the_speaker_with_most_overlap():
    turns = [SpeakerTurn(0, 0.2, "speaker_1"), SpeakerTurn(0.2, 1, "speaker_2")]
    assert speaker_at(0, 1, turns) == "speaker_2"


def test_missing_word_times_preserve_text_without_guessing_a_speaker():
    raw = [{"start": 0, "end": 2, "text": "Unaligned speech."}]
    segments = label_words(raw, [SpeakerTurn(0, 2, "speaker_1")])
    assert segments[0].text == raw[0]["text"]
    assert segments[0].speaker is None


def test_transcription_requests_word_times_and_passes_speaker_hints(
    tmp_path, monkeypatch
):
    audio = [0.1, 0.2]
    monkeypatch.setattr("podcast.transcribe.diarizer_binary", Mock())
    whisper = Mock(
        transcribe=Mock(
            return_value={
                "language": "en",
                "segments": [
                    {
                        "start": 0,
                        "end": 1,
                        "text": "I'm Alex.",
                        "words": [{"start": 0, "end": 1, "word": "I'm Alex."}],
                    }
                ],
            }
        )
    )
    monkeypatch.setitem(sys.modules, "mlx_whisper", whisper)
    monkeypatch.setitem(
        sys.modules, "mlx_whisper.audio", Mock(load_audio=Mock(return_value=audio))
    )
    diarizer = Mock(return_value=[SpeakerTurn(0, 1, "speaker_1")])
    monkeypatch.setattr("podcast.transcribe.diarize", diarizer)
    result = transcribe(
        tmp_path / "video.mp4", num_speakers=3, speaker_names=("Alex", "Sam", "Jo")
    )
    assert result.segments[0].speaker == "speaker_1"
    assert whisper.transcribe.call_args.kwargs["word_timestamps"] is True
    assert (
        whisper.transcribe.call_args.kwargs["initial_prompt"]
        == "Participants: Alex, Sam, Jo"
    )
    diarizer.assert_called_once_with(tmp_path / "video.mp4", 3)
