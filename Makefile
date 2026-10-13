.PHONY: lint lint-verify lint-python lint-python-verify lint-swift lint-swift-verify

UV ?= uv
SWIFTLINT ?= swiftlint

lint: lint-python lint-swift

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
