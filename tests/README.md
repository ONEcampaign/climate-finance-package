# Climate Finance Testing Suite

## Running Tests

### Run all tests
```bash
pytest tests/ -v
```

### Run only unit tests (fast)
```bash
pytest tests/unit/ -v
```

### Run only integration tests
```bash
pytest tests/integration/ -v -m integration
```

### Run validation tests (slow, requires real data)
```bash
pytest tests/integration/test_oecd_validation.py -v -m slow
```

### Run excluding slow tests
```bash
pytest -m "not slow" -v
```

## Test Structure

- `tests/unit/` - Fast, isolated unit tests for logic
- `tests/integration/` - Workflow tests using ClimateData API
- `tests/fixtures/` - Test data (builders, CSVs, benchmarks)
- `conftest.py` - Shared pytest fixtures

## Adding New Tests

### Unit Tests
1. Create test file in appropriate `tests/unit/` subdirectory
2. Use builders from `tests/fixtures/builders.py` for test data
3. Test one logical unit per test function
4. Keep tests fast (<50ms)

### Integration Tests
1. Create test in `tests/integration/`
2. Mark with `@pytest.mark.integration`
3. Use small real data samples or mock external calls
4. Test complete workflows

### Validation Tests
1. Add benchmark data to `tests/fixtures/oecd_benchmarks_2020.json`
2. Mark with `@pytest.mark.slow`
3. Document expected values and sources

## Updating Fixtures

### OECD Benchmarks
When OECD publishes new data:
1. Update `tests/fixtures/oecd_benchmarks_20XX.json`
2. Document source and extraction date
3. Run validation tests to verify

### Test Data
To add new synthetic data:
1. Add builder function to `tests/fixtures/builders.py`, or
2. Add CSV to `tests/fixtures/synthetic/`

## CI/CD

- **Every commit:** Unit tests run
- **Pull requests:** Unit + integration tests
- **Nightly:** Validation tests with real data

## Regression Testing with Snapshots

Snapshot tests freeze outputs and detect unintended changes.

**Note:** Current snapshot tests use real OECD data (~1GB download on first run) and are marked with `@pytest.mark.slow`. They will be skipped when running `pytest -m "not slow"`.

### First run (create snapshot)
```bash
pytest tests/regression/test_snapshots.py --snapshot-update -m slow
```

### Subsequent runs (verify against snapshot)
```bash
pytest tests/regression/test_snapshots.py -m slow
```

### Updating snapshots after intentional changes
```bash
pytest tests/regression/test_snapshots.py --snapshot-update -m slow
```

Always review snapshot diffs carefully before updating!

### Requirements
- pytest-snapshot must be installed:
  ```bash
  # Install dev dependencies (includes pytest, pytest-cov, pytest-snapshot)
  uv sync --group dev
  ```
- Snapshot tests currently require actual OECD data downloads (~1GB)
- Future improvements could mock data at the oda_data level for faster testing

## Test Coverage

### Overall Coverage: 37%

**Last Updated:** 2025-10-28

### Test Results Summary
- **Total Tests:** 55 passed, 2 deselected (slow tests)
- **Test Execution Time:** ~4.5 minutes (excluding slow tests)
- **Total Statements:** 2,260
- **Covered Statements:** 826
- **Missing Statements:** 1,434

### Coverage by Module

#### High Coverage (>80%)
- `climate_finance/__init__.py`: 100%
- `climate_finance/common/schema.py`: 100%
- `climate_finance/config.py`: 100%
- `climate_finance/methodologies/spending/crs.py`: 100%
- `climate_finance/methodologies/spending/tools.py`: 100%
- `climate_finance/oecd/cleaning_tools/settings.py`: 100%
- `climate_finance/core/loaders.py`: 85%

#### Medium Coverage (50-80%)
- `climate_finance/core/enums.py`: 71%
- `climate_finance/common/analysis_tools.py`: 69%
- `climate_finance/core/data.py`: 52%
- `climate_finance/oecd/crs/get_data.py`: 51%
- `climate_finance/core/validation.py`: 51%
- `climate_finance/core/deflators.py`: 50%
- `climate_finance/core/dtypes.py`: 50%

#### Low Coverage (<50%)
- `climate_finance/core/tools.py`: 46%
- `climate_finance/oecd/crdf/recipient_perspective.py`: 46%
- `climate_finance/unfccc/cleaning_tools/tools.py`: 42%
- `climate_finance/oecd/cleaning_tools/tools.py`: 37%
- `climate_finance/unfccc/download/pre_process.py`: 34%
- `climate_finance/oecd/crdf/provider_perspective.py`: 33%
- `climate_finance/oecd/cleaning_tools/names.py`: 31%
- `climate_finance/oecd/crdf/tools.py`: 30%
- `climate_finance/methodologies/spending/crdf_crs.py`: 29%
- `climate_finance/methodologies/spending/crdf.py`: 27%
- `climate_finance/methodologies/multilateral/tools.py`: 25%

#### Not Covered (0%)
- `climate_finance/for_testing.py`: 0% (intentional - testing utilities)
- `climate_finance/methodologies/bilateral/tools.py`: 0%
- `climate_finance/methodologies/multilateral/crs_tools.py`: 0%
- `climate_finance/methodologies/multilateral/oecd_multilateral/get_oecd_imputations.py`: 0%
- `climate_finance/methodologies/multilateral/one_multilateral/shares.py`: 0%
- `climate_finance/oecd/crs/add_crs_data.py`: 0%
- `climate_finance/oecd/multisystem/get_data.py`: 0%
- `climate_finance/unfccc/manual/get_data.py`: 0%
- `climate_finance/unfccc/manual/pre_process.py`: 0%
- `climate_finance/unfccc/manual/read_files.py`: 0%
- `climate_finance/validation/matching_methodology.py`: 0%
- Various other low-usage modules in UNFCCC and download paths

### What's Well Tested

Current test coverage focuses on:
- ✅ **Methodology transformations** (OECD vs ONE coefficients) - 100% coverage
- ✅ **Data schema and configuration** - 100% coverage
- ✅ **Core spending methodologies** - 100% coverage
- ✅ **Multilateral imputations** (shares, calculations) - tested with fixtures
- ✅ **Data filtering and aggregation utilities** - comprehensive unit tests
- ✅ **ClimateData API workflows** - integration tests
- ✅ **OECD CRS data cleaning** - multiple edge cases covered
- ✅ **Test fixtures and builders** - synthetic data generation

### Areas Needing More Coverage

Priority areas for improvement:
1. **Core tools** (46% coverage) - aggregation and transformation utilities
2. **UNFCCC data processing** (18-42% coverage) - download, cleaning, validation
3. **OECD CRDF** (27-33% coverage) - provider/recipient perspectives
4. **Bilateral methodologies** (0% coverage) - completely untested
5. **Multilateral OECD/ONE implementations** (0% coverage) - need integration tests
6. **Manual data processing** (0% coverage) - UNFCCC manual workflows
7. **Validation frameworks** (0% coverage) - matching methodologies
8. **Data addition utilities** (0% coverage) - CRS data addition workflows

### Areas Intentionally Not Tested
- Currency deflation (relies on pydeflate library)
- External data downloads (mocked in tests)
- Network-dependent operations (mocked with fixtures)

### Viewing Coverage Reports

To generate and view HTML coverage report:
```bash
pytest tests/ -v -m "not slow" --cov=climate_finance --cov-report=html:.reports/coverage
open .reports/coverage/index.html  # macOS
xdg-open .reports/coverage/index.html  # Linux
```

### Coverage Goals
- **Current:** 37%
- **Short-term target:** 60% (add multilateral and bilateral tests)
- **Long-term target:** 80% for core business logic
- **Note:** Some modules (UNFCCC manual, validation) may remain at lower coverage due to their specialized/infrequent use
