# Portfolios Tracker Testing Suite

## Overview

This directory contains comprehensive automated tests for the Portfolios Tracker project, covering unit tests, integration tests, and data quality validation.

## Test Structure

```
tests/
├── __init__.py
├── conftest.py                 # Shared pytest fixtures
├── unit/                       # Unit tests for individual functions
│   ├── test_fetcher.py        # Tests for ETL data fetching
│   └── test_notifications.py  # Tests for notification system
├── integration/                # Integration tests
│   └── (to be added)
├── fixtures/                   # Sample test data
│   └── sample_data.json       # Stock data samples
└── mocks/                      # Mock utilities
    ├── vnstock_mock.py        # Mock vnstock API
    └── api_responses.py       # Mock Telegram/Gemini responses
```

## Running Tests

### Run All Tests

```bash
# From project root
pytest

# Or inside Docker container
docker exec -it portfolios-tracker-airflow-worker-1 pytest
```

### Run Specific Test File

```bash
pytest tests/unit/test_fetcher.py
pytest tests/unit/test_notifications.py
```

### Run with Coverage Report

```bash
pytest --cov=dags --cov-report=html
# View report at htmlcov/index.html
```

### Run Tests by Marker

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"
```

## Test Categories

### Unit Tests (`@pytest.mark.unit`)

Test individual functions in isolation with mocked external dependencies.

**test_fetcher.py:**

- `clean_decimal_cols()` - Data cleaning for Decimal types
- `fetch_stock_price()` - Stock price fetching with indicators
- `fetch_financial_ratios()` - Financial ratios extraction
- `fetch_income_stmt()` - Income statement data (TODO)
- `fetch_dividends()` - Dividend history (TODO)
- `fetch_news()` - News article fetching (TODO)

**test_notifications.py:**

- `send_telegram_message()` - Telegram notification sending
- `send_success_notification()` - Success callback wrapper
- `send_failure_notification()` - Failure callback wrapper
- `send_telegram_news_summary()` - News digest with AI
- `get_latest_stock_data()` - ClickHouse data retrieval
- `summarize_news_with_gemini()` - AI summary generation

### Integration Tests (`@pytest.mark.integration`)

Test end-to-end workflows with real or test databases.

**Status:** TODO - To be implemented in Phase 2

### Data Quality Tests

Validate data integrity in ClickHouse after ETL runs.

**Status:** TODO - To be implemented in Phase 3

## Writing New Tests

### Basic Test Template

```python
import pytest
from unittest.mock import patch

@pytest.mark.unit
def test_my_function():
    """Test description."""
    # Arrange
    input_data = ...

    # Act
    result = my_function(input_data)

    # Assert
    assert result == expected_value
```

### Using Fixtures

```python
@pytest.mark.unit
def test_with_fixture(sample_stock_price_df):
    """Test using predefined fixture."""
    assert not sample_stock_price_df.empty
```

### Mocking External APIs

```python
@patch('module.vnstock.Quote')
def test_with_mock(mock_quote_class):
    """Test with mocked vnstock API."""
    mock_quote = Mock()
    mock_quote.history.return_value = pd.DataFrame(...)
    mock_quote_class.return_value = mock_quote

    result = fetch_stock_price('HPG', '2024-01-01', '2024-01-10')
    assert not result.empty
```

## Available Fixtures

See [conftest.py](conftest.py) for all available fixtures:

- `mock_environment_variables` - Mock env vars (auto-applied)
- `mock_clickhouse_client` - Mock ClickHouse client
- `sample_stock_price_df` - Sample OHLCV data with indicators
- `sample_financial_ratios_df` - Sample quarterly ratios
- `sample_income_statement_df` - Sample P&L data
- `sample_dividends_df` - Sample dividend history
- `sample_news_df` - Sample news articles
- `sample_airflow_context` - Mock Airflow task context
- `mock_telegram_response` - Mock Telegram API response
- `mock_gemini_response` - Mock Gemini AI response

## Coverage Goals

- **Target:** 80% code coverage minimum
- **Current Status:** Phase 1 implemented (foundation + core functions)
- **Priority Areas:**
  - ✅ `clean_decimal_cols()` - 100% coverage
  - ✅ Notification callbacks - 100% coverage
  - ⚠️ `fetch_stock_price()` - Partial coverage (core scenarios)
  - ⚠️ `fetch_financial_ratios()` - Partial coverage
  - ❌ Other fetch functions - Placeholder tests only

## CI/CD Integration

**Status:** TODO - Add GitHub Actions workflow

Planned workflow:

```yaml
# .github/workflows/test.yml
- Run pytest on every PR
- Generate coverage report
- Block merge if coverage < 80%
- Upload coverage to Codecov
```

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError: No module named 'etl_modules'`:

```bash
# Tests expect to be run from project root
export PYTHONPATH=/opt/airflow:$PYTHONPATH
pytest
```

### Docker Container Tests

```bash
# Install test dependencies in container
docker exec -it portfolios-tracker-airflow-worker-1 \
  pip install -r requirements.txt

# Run tests
docker exec -it portfolios-tracker-airflow-worker-1 pytest
```

### Mock API Not Working

Ensure `responses` library is properly installed and activated:

```python
import responses

@responses.activate
def test_api_call():
    responses.add(responses.POST, url, json={...})
    # Your test code
```

## Best Practices

1. **Mock External Dependencies** - Never call real APIs in tests
2. **Use Descriptive Names** - Test names should describe what they test
3. **Follow AAA Pattern** - Arrange, Act, Assert
4. **Test Edge Cases** - Empty data, None values, errors
5. **Keep Tests Fast** - Unit tests should run in milliseconds
6. **Isolate Tests** - Each test should be independent

## Next Steps (Roadmap)

### Phase 2: Expand Unit Tests

- Complete `fetch_income_stmt()` tests
- Complete `fetch_dividends()` tests
- Complete `fetch_news()` tests
- Add error handling scenarios
- Test data transformation edge cases

### Phase 3: Integration Tests

- DAG structure validation
- Task dependency validation
- End-to-end pipeline tests
- ClickHouse schema validation

### Phase 4: Data Quality Tests

- Duplicate record detection
- Data range validation
- Technical indicator verification
- Cross-table consistency checks

### Phase 5: CI/CD Setup

- GitHub Actions workflow
- Automated test runs on PR
- Coverage reporting
- Pre-commit hooks

## Contributing

When adding new functionality:

1. Write tests first (TDD approach recommended)
2. Ensure all tests pass: `pytest`
3. Check coverage: `pytest --cov=dags`
4. Add docstrings to test functions
5. Update this README if adding new test categories

## References

- [pytest documentation](https://docs.pytest.org/)
- [pytest-mock](https://pytest-mock.readthedocs.io/)
- [responses library](https://github.com/getsentry/responses)
- [Airflow testing best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-dags)
