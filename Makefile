.PHONY: setup-diarizer lint lint-verify lint-python lint-python-verify lint-swift lint-swift-verify

UV ?= uv
SWIFTLINT ?= swiftlint
FLUIDAUDIO_REVISION := 793a2c5a0e99705a55fefbad00e65ca684329349

lint: lint-python lint-swift

setup-diarizer:
	mkdir -p .cache
	test -d .cache/fluidaudio/.git || git clone --no-checkout --depth 1 https://github.com/FluidInference/FluidAudio.git .cache/fluidaudio
	git -C .cache/fluidaudio fetch --depth 1 origin $(FLUIDAUDIO_REVISION)
	git -C .cache/fluidaudio checkout --detach $(FLUIDAUDIO_REVISION)
	swift build --package-path .cache/fluidaudio -c release --product fluidaudiocli --disable-default-traits

lint-verify: lint-python-verify lint-swift-verify

lint-python:
	$(UV) run ruff format podcast tests
	$(UV) run ruff check podcast tests --fix
	cd pipeline && $(UV) tool run --from ruff ruff format .
	cd pipeline && $(UV) tool run --from ruff ruff check . --fix

lint-python-verify:
	$(UV) run ruff format --check podcast tests
	$(UV) run ruff check podcast tests
	$(UV) run pytest
	cd pipeline && $(UV) tool run --from ruff ruff format --check .
	cd pipeline && $(UV) tool run --from ruff ruff check .

lint-swift:
	cd mirror && $(SWIFTLINT) lint --fix
	cd mirror && $(SWIFTLINT) lint --strict

lint-swift-verify:
	cd mirror && $(SWIFTLINT) lint --strict
