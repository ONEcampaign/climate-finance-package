include default.mk

SRC = oda_data tests

report-coverage: .venv
	@echo '==> Unit testing with coverage'
	poetry run pytest --cov=climate_finance --cov-report=term-missing --cov-report=html:.reports/coverage --cov-report=xml tests

report: report-coverage report-server-launch
	@poetry run python -m http.server .reports/

report-server-launch: .venv
	@echo '==> Showing reports'
	@poetry run python -m http.server --directory .reports/

bump-patch: .venv
	@echo '==> Bumping version (patch)'
	poetry run bump2version patch $(filter-out $@, $(MAKECMDGOALS))