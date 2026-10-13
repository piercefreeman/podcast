# podcast
Open source utilities for [The Pretrained Pod](https://pretrained.fm).

## Episode summary

Transcribe a local video or remote URL on Apple Silicon with MLX Whisper
`large-v3-turbo`, then send the transcript to OpenAI `gpt-6-astra` through a
PydanticAI agent for a producer brief.

```bash
brew install ffmpeg
cp -n .env.example .env  # Set OPENAI_API_KEY (keep .env private).
uv sync
uv run episode-summary ~/Downloads/086.mp4 -o output/086
uv run episode-summary 'https://example.com/episode.mp4' -o output/remote
uv run episode-summary 'https://f.io/YOUR_SHARE' -o output/frameio
```

Remote URLs use yt-dlp, including direct video links and supported video sites.
Public Frame.io shares that open a single video are also supported when downloads
are enabled; the resolver uses Frame.io's web API and prefers the audio HLS stream.
Only a single video is processed; authentication/DRM and playlists are not supported.
Downloaded audio is cached as 16 kHz mono WAV in `.cache/audio/` for seven days.
`index.json` maps each original URL to its local filename and UTC `expires_at`.
An unexpired entry is reused without a network request. Expired, missing, or empty
files are downloaded again; failed downloads never enter the index. The index is
updated atomically under a file lock, so simultaneous runs cannot corrupt it.
Expiry triggers refresh on the next use, rather than deleting files in the background.
Remove `.cache/audio/` to clear the cache. Share URLs and audio are private, ignored
local files. Temporary media is removed after audio extraction. The existing Hugging
Face model cache is reused; the first run downloads the model if it is missing.

Each run writes `transcript.json`, timestamped `transcript.txt`, structured
`summary.json`, and a readable `summary.md`. The brief includes a synopsis,
takeaways, a chronological rundown, clip candidates, and actionable producer
notes. Its timestamps come from validated transcript segment references.
Choose a fresh output directory for each run; existing files are never overwritten.
The transcript remains available if the API request fails.

Audio stays local; transcript text is sent to OpenAI. Whisper does not identify
speakers or inspect visuals, and its timestamps/names should be checked before
publishing. An OpenAI API key with access to Astra is required. `.env` is loaded
from the current directory; existing environment variables take precedence.

```bash
uv run pytest
uv run ruff check podcast tests
uv run ruff format --check podcast tests
```

## Pipeline

Our post-record pipeline. We record on-device with a series of camcorders and audio recorders. This pipeline automates aligning media files from different sources, moving to our NAS, converting with ffmpeg, and uploading to Frame.

```bash
uv run --project pipeline pipeline
```

## Mirror

Utility to invert our Riverside recording window, so we can broadcast the flipped secondary window to our teleprompter (so I can look at the camera while I'm talking to Richard). Our Makefile takes care of building a production-ready .app versus the Swift CLI utilities that only output an executable.

```bash
tccutil reset ScreenCapture com.pretrainedpod.mirror
make run
```
