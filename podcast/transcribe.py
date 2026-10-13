from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

WHISPER_MODEL = "mlx-community/whisper-large-v3-turbo"


class Segment(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, allow_inf_nan=False)

    start: float = Field(ge=0)
    end: float = Field(ge=0)
    text: str = Field(min_length=1)

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


def transcribe(path: Path) -> Transcript:
    # Import on demand so CLI help and summary tests do not initialize Metal.
    import mlx_whisper

    result = mlx_whisper.transcribe(
        str(path),
        path_or_hf_repo=WHISPER_MODEL,
        condition_on_previous_text=False,
        verbose=False,
    )
    segments = [
        Segment.model_validate(segment)
        for segment in result["segments"]
        if segment["text"].strip()
    ]
    if not segments:
        raise ValueError("Whisper found no speech; no summary was requested.")
    return Transcript(language=result["language"], segments=segments)
