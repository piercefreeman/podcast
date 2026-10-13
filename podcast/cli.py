import os
import shutil
from pathlib import Path

import click
from dotenv import load_dotenv

from .transcribe import timestamp, transcribe
from .video import VideoSource


@click.command()
@click.argument("video")
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    default="output/episode",
    show_default=True,
    help="Directory for the transcript and producer brief (never overwrites).",
)
def main(video: str, output_dir: Path) -> None:
    """Transcribe a local VIDEO or URL with Whisper, then summarize with Astra."""
    load_dotenv(Path.cwd() / ".env")
    os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")
    if not os.getenv("OPENAI_API_KEY"):
        raise click.ClickException("Set OPENAI_API_KEY in your environment or .env.")
    if not shutil.which("ffmpeg"):
        raise click.ClickException(
            "ffmpeg is required. Install it with: brew install ffmpeg"
        )
    try:
        source = VideoSource.from_input(video)
        outputs = [
            output_dir / name
            for name in (
                "transcript.json",
                "transcript.txt",
                "summary.json",
                "summary.md",
            )
        ]
        if any(path.exists() for path in outputs):
            raise ValueError(
                f"Output files already exist in {output_dir}; choose another --output-dir."
            )
        output_dir.mkdir(parents=True, exist_ok=True)
        click.echo(
            "Preparing video and transcribing locally with MLX Whisper turbo…", err=True
        )
        with source.local_path() as path:
            transcript = transcribe(path)
        with outputs[0].open("x") as file:
            file.write(transcript.model_dump_json(indent=2) + "\n")
        with outputs[1].open("x") as file:
            file.write(
                "\n".join(
                    f"[{timestamp(s.start)}–{timestamp(s.end)}] {s.text}"
                    for s in transcript.segments
                )
                + "\n"
            )
        click.echo(
            "Transcript saved. Sending transcript text to Astra for the producer brief…",
            err=True,
        )
        from .summary import render_summary, summarize

        summary = summarize(transcript)
        with outputs[2].open("x") as file:
            file.write(summary.model_dump_json(indent=2) + "\n")
        with outputs[3].open("x") as file:
            file.write(render_summary(summary, transcript))
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(str(outputs[3].resolve()))
