## Pipeline CLI

```bash
uv run pipeline
```

Common options:

- `--start-window 5min` (default)
- `--podcast-root /Volumes/Common_Drive/podcast`
- `--yes` to skip prompts
- `--dry-run` to preview actions
- `--skip-frameio-upload` to skip Frame.io upload after conversion

Frame.io credentials are loaded via `pydantic-settings` with a vault fallback.
The settings model uses `vaultdantic` with 1Password by default:

- Vault: `Side-Projects`
- Item: `Pretrained-Pipeline`

Frame.io settings are validated at CLI startup (including `--dry-run`) before scanning/copying begins.
