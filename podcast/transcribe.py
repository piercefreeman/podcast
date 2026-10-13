from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .diarize import SpeakerTurn, diarize, diarizer_binary, speaker_at

WHISPER_MODEL = "mlx-community/whisper-large-v3-turbo"


class Segment(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, allow_inf_nan=False)

    start: float = Field(ge=0)
    end: float = Field(ge=0)
    text: str = Field(min_length=1)
    speaker: str | None = None

    @model_validator(mode="after")
    def valid_range(self) -> "Segment":
        if self.end < self.start:
            raise ValueError("A transcript segment ends before it starts.")
        return self


class Transcript(BaseModel):
    language: str
    segments: list[Segment] = Field(min_length=1)


def timestamp(seconds: float) -> str:
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def label_words(raw_segments: list[dict], turns: list[SpeakerTurn]) -> list[Segment]:
    segments = []
    for raw in raw_segments:
        if not raw["text"].strip():
            continue
        words = raw.get("words", [])
        if not words:
            segments.append(Segment.model_validate(raw))
            continue
        current = None
        for word in words:
            speaker = speaker_at(word["start"], word["end"], turns)
            if current is None or current.speaker != speaker:
                if current is not None:
                    segments.append(current)
                current = Segment(
                    start=word["start"],
                    end=word["end"],
                    text=word["word"],
                    speaker=speaker,
                )
            else:
                current.end = word["end"]
                current.text += word["word"]
        if current is not None:
            segments.append(current)
    return segments


def transcribe(
    path: Path, num_speakers: int | None = None, speaker_names: tuple[str, ...] = ()
) -> Transcript:
    diarizer_binary()
    # Import on demand so CLI help and summary tests do not initialize Metal.
    import mlx_whisper
    from mlx_whisper.audio import load_audio

    audio = load_audio(str(path))
    result = mlx_whisper.transcribe(
        audio,
        path_or_hf_repo=WHISPER_MODEL,
        condition_on_previous_text=False,
        verbose=False,
        word_timestamps=True,
        initial_prompt=("Participants: " + ", ".join(speaker_names))
        if speaker_names
        else None,
    )
    if not any(segment["text"].strip() for segment in result["segments"]):
        raise ValueError("Whisper found no speech; no summary was requested.")
    segments = label_words(result["segments"], diarize(path, num_speakers))
    return Transcript(language=result["language"], segments=segments)
