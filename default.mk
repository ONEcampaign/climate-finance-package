SRC = climate_finance tests

.venv: pyproject.toml poetry.toml poetry.lock
	poetry install
	touch $@

# check formatting before lint, since an autoformat might fix linting issues
test-default: check-formatting unittest

check-formatting: .venv
	@echo '==> Checking formatting'
	@poetry run black --check $(SRC)

unittest: .venv
	@echo '==> Running unit tests'
	@PYTHONPATH=. poetry run pytest

format-default: .venv
	@echo '==> Reformatting files'
	@poetry run black $(SRC)