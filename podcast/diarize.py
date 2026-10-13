import json
import math
import subprocess
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from .video import extract_audio


@dataclass(frozen=True)
class SpeakerTurn:
    start: float
    end: float
    speaker: str

    def __post_init__(self):
        if not (
            math.isfinite(self.start)
            and math.isfinite(self.end)
            and 0 <= self.start < self.end
        ):
            raise ValueError("Invalid speaker turn timestamps.")


def diarizer_binary() -> Path:
    binary = Path(".cache/fluidaudio/.build/release/fluidaudiocli")
    if not binary.is_file():
        raise ValueError(
            "Run 'make setup-diarizer' once to build the local CoreML speaker model runner."
        )
    return binary.resolve()


def diarize(path: Path, num_speakers: int | None = None) -> list[SpeakerTurn]:
    binary = diarizer_binary()
    with TemporaryDirectory(prefix="podcast-speakers-") as directory:
        audio = Path(directory) / "audio.wav"
        output = Path(directory) / "speakers.json"
        extract_audio(str(path), audio)
        count = (
            ["--num-speakers", str(num_speakers)]
            if num_speakers is not None
            else ["--min-speakers", "2", "--max-speakers", "3"]
        )
        result = subprocess.run(
            [
                str(binary),
                "process",
                str(audio),
                "--mode",
                "offline",
                "--compute-units",
                "ane",
                "--step-ratio",
                "0.1",
                "--min-segment-duration",
                "0.2",
                "--output",
                str(output),
                *count,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode:
            raise ValueError(f"Speaker separation failed: {result.stderr.strip()}")
        segments = json.loads(output.read_text())["segments"]
    labels = {}
    turns = []
    for turn in sorted(segments, key=lambda s: s["startTimeSeconds"]):
        label = labels.setdefault(turn["speakerId"], f"speaker_{len(labels) + 1}")
        turns.append(
            SpeakerTurn(turn["startTimeSeconds"], turn["endTimeSeconds"], label)
        )
    if not turns:
        raise ValueError("The speaker model found no speech to label.")
    return turns


def speaker_at(start: float, end: float, turns: list[SpeakerTurn]) -> str | None:
    # shortcut: Whisper timing can blur short turns; add forced alignment for finer attribution.
    overlaps: dict[str, float] = {}
    for turn in turns:
        if turn.start >= end:
            break
        overlap = max(0.0, min(end, turn.end) - max(start, turn.start))
        if overlap:
            overlaps[turn.speaker] = overlaps.get(turn.speaker, 0.0) + overlap
    ranked = sorted(overlaps, key=overlaps.get, reverse=True)
    if not ranked or overlaps[ranked[0]] <= (end - start) / 2:
        return None
    if len(ranked) > 1 and overlaps[ranked[1]] >= (end - start) / 2:
        return None
    return ranked[0]
