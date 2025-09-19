include default.mk

SRC = climate_finance tests

report-coverage: .venv
	@echo '==> Unit testing with coverage'
	uv run pytest --cov=climate_finance --cov-report=term-missing --cov-report=html:.reports/coverage --cov-report=xml tests

report: report-coverage report-server-launch
	@uv run python -m http.server .reports/

report-server-launch: .venv
	@echo '==> Showing reports'
	@uv run python -m http.server --directory .reports/

bump-patch: .venv
	@echo '==> Bumping version (patch)'
	uv run bump2version patch $(filter-out $@, $(MAKECMDGOALS))