SRC = climate_finance tests

.venv: pyproject.toml uv.lock
	uv sync
	touch $@

# check formatting before lint, since an autoformat might fix linting issues
test-default: check-formatting unittest

check-formatting: .venv
	@echo '==> Checking formatting'
	@uv run black --check $(SRC)

format: .venv
	@echo '==> Reformatting files'
	@uv run black $(SRC)

unittest: .venv
	@echo '==> Running unit tests'
	@PYTHONPATH=. uv run pytest

format-default: .venv
	@echo '==> Reformatting files'
	@uv run black $(SRC)