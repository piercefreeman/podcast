.PHONY: lint lint-verify lint-python lint-python-verify lint-swift lint-swift-verify

UV ?= uv
SWIFTLINT ?= swiftlint

lint: lint-python lint-swift

lint-verify: lint-python-verify lint-swift-verify

lint-python:
	cd pipeline && $(UV) tool run --from ruff ruff format .
	cd pipeline && $(UV) tool run --from ruff ruff check . --fix

lint-python-verify:
	cd pipeline && $(UV) tool run --from ruff ruff format --check .
	cd pipeline && $(UV) tool run --from ruff ruff check .

lint-swift:
	cd mirror && $(SWIFTLINT) lint --fix
	cd mirror && $(SWIFTLINT) lint --strict

lint-swift-verify:
	cd mirror && $(SWIFTLINT) lint --strict
