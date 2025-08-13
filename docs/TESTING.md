# 🧪 Testing Guide

## Overview

This project includes a comprehensive test suite covering all major functionality including CLI operations, data migration, dataset comparison, inventory management, and maintenance scripts.

## Test Structure

```
tests/
├── __init__.py                 # Test package initialization
├── conftest.py                 # Shared fixtures and configuration
├── test_cli.py                 # CLI interface tests
├── test_dataset_comparator.py  # Data comparison tests
├── test_inventory_handler.py   # Inventory management tests
├── test_migration.py           # Data migration tests
├── test_utils.py               # Utility function tests
├── test_maintenance.py         # Maintenance script tests
├── coverage_html/              # HTML coverage reports (generated)
├── .pytest_cache/              # Pytest cache (generated)
└── report.html                 # HTML test report (generated)
```

## Running Tests

### Quick Start

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
python run_tests.py

# Or use pytest directly
pytest
```

### Test Categories

```bash
# Run only unit tests (fast)
python run_tests.py --unit
pytest -m unit

# Run integration tests
python run_tests.py --integration
pytest -m integration

# Run tests for specific module
python run_tests.py --module cli
pytest tests/test_cli.py

# Run with coverage
python run_tests.py --coverage
pytest --cov=tools --cov-report=html
```

### Advanced Options

```bash
# Parallel execution
python run_tests.py --parallel
pytest -n auto

# Verbose output
python run_tests.py --verbose
pytest -v

# Skip slow tests
python run_tests.py --fast
pytest -m "not slow"

# Generate reports
python run_tests.py --html-report --xml-report
pytest --html=tests/report.html --junitxml=tests/junit.xml
```

## Test Markers

Tests are organized using pytest markers:

- `@pytest.mark.unit` - Fast, isolated unit tests
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.slow` - Tests that take >5 seconds
- `@pytest.mark.api` - Tests requiring API connections
- `@pytest.mark.database` - Tests requiring database access
- `@pytest.mark.sheets` - Tests requiring Google Sheets
- `@pytest.mark.cli` - Command-line interface tests
- `@pytest.mark.migration` - Data migration tests
- `@pytest.mark.comparison` - Data comparison tests
- `@pytest.mark.maintenance` - Maintenance script tests

## Writing Tests

### Test Structure

```python
import pytest
from unittest.mock import Mock, patch

class TestYourComponent:
    """Test YourComponent functionality."""
    
    def test_basic_functionality(self):
        """Test basic functionality."""
        # Arrange
        component = YourComponent()
        
        # Act
        result = component.do_something()
        
        # Assert
        assert result is True
    
    @pytest.mark.slow
    def test_slow_operation(self):
        """Test slow operation."""
        # This test takes a long time
        pass
    
    @patch('your_module.ExternalService')
    def test_with_mock(self, mock_service):
        """Test with mocked external service."""
        mock_service.return_value.get_data.return_value = "test_data"
        
        component = YourComponent()
        result = component.process_data()
        
        assert result == "processed_test_data"
```

### Using Fixtures

```python
def test_with_temp_directory(temp_dir):
    """Test using temporary directory fixture."""
    test_file = os.path.join(temp_dir, 'test.txt')
    with open(test_file, 'w') as f:
        f.write('test content')
    
    assert os.path.exists(test_file)

def test_with_mock_data(mock_google_sheets_data):
    """Test using mock Google Sheets data."""
    assert len(mock_google_sheets_data) > 0
    assert 'Dataset ID' in mock_google_sheets_data[0]
```

### Available Fixtures

See `tests/conftest.py` for all available fixtures:

- `temp_dir` - Temporary directory
- `mock_env_vars` - Mock environment variables
- `mock_google_sheets_data` - Sample Google Sheets data
- `mock_domo_dataset` - Mock Domo dataset structure
- `mock_snowflake_table` - Mock Snowflake table structure
- `mock_polars_dataframe` - Mock Polars DataFrame
- `mock_pandas_dataframe` - Mock Pandas DataFrame
- `mock_domo_handler` - Mock DomoHandler
- `mock_snowflake_handler` - Mock SnowflakeHandler
- `mock_google_sheets_handler` - Mock GoogleSheets handler
- `mock_dataset_comparator` - Mock DatasetComparator
- `sample_json_mapping` - Sample JSON mapping data
- `sample_sql_content` - Sample SQL content

## Coverage Requirements

- Minimum coverage: 80%
- Target coverage: 90%+
- All public methods should be tested
- Critical paths must have >95% coverage

### Checking Coverage

```bash
# Generate coverage report
pytest --cov=tools --cov-report=html

# View HTML report
open tests/coverage_html/index.html

# Terminal coverage report
pytest --cov=tools --cov-report=term-missing
```

## CI/CD Integration

### GitHub Actions

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, "3.10", "3.11"]
    
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install -r requirements-test.txt
    
    - name: Run tests
      run: |
        pytest --cov=tools --cov-report=xml --junitxml=junit.xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

### GitLab CI

```yaml
test:
  stage: test
  image: python:3.9
  script:
    - pip install -r requirements.txt
    - pip install -r requirements-test.txt
    - pytest --cov=tools --cov-report=xml --junitxml=junit.xml
  coverage: '/TOTAL.*\s+(\d+%)$/'
  artifacts:
    reports:
      junit: junit.xml
      coverage_report:
        coverage_format: cobertura
        path: coverage.xml
```

## Mock Strategies

### External APIs

```python
@patch('tools.utils.domo.pydomo.Domo')
def test_domo_connection(mock_domo_class):
    """Test Domo connection with mocked API."""
    mock_client = Mock()
    mock_client.datasets.list.return_value = [{'id': '123', 'name': 'Test'}]
    mock_domo_class.return_value = mock_client
    
    handler = DomoHandler()
    datasets = handler.list_all_datasets()
    
    assert len(datasets) == 1
```

### Database Connections

```python
@patch('tools.utils.snowflake.snowflake.connector.connect')
def test_snowflake_connection(mock_connect):
    """Test Snowflake connection with mocked connector."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [('result',)]
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    handler = SnowflakeHandler()
    handler.connection = mock_conn
    result = handler.execute_query("SELECT 1")
    
    assert result is not None
```

### File Operations

```python
from unittest.mock import mock_open

@patch('builtins.open', mock_open(read_data='test,data\n1,2'))
def test_file_reading():
    """Test file reading with mocked file content."""
    with open('test.csv', 'r') as f:
        content = f.read()
    
    assert 'test,data' in content
```

## Testing Best Practices

### 1. Test Naming

- Use descriptive test names: `test_migration_with_invalid_dataset_id_should_fail`
- Group related tests in classes: `TestDatasetComparator`
- Use consistent naming patterns

### 2. Test Organization

- One test class per component/module
- Group tests by functionality
- Use clear docstrings

### 3. Assertions

```python
# Good: Specific assertions
assert result.status == 'success'
assert len(result.data) == 3
assert 'customer_id' in result.columns

# Avoid: Generic assertions
assert result
assert result is not None
```

### 4. Test Data

- Use fixtures for reusable test data
- Keep test data minimal but realistic
- Use factories for complex objects

### 5. Mocking

- Mock external dependencies (APIs, databases, files)
- Don't mock the code under test
- Use specific return values, not just `Mock()`

### 6. Error Testing

```python
def test_handles_connection_error():
    """Test that connection errors are handled gracefully."""
    with patch('tools.utils.domo.pydomo.Domo') as mock_domo:
        mock_domo.side_effect = ConnectionError("API unavailable")
        
        handler = DomoHandler()
        result = handler.test_connection()
        
        assert result is False
```

## Debugging Tests

### Running Single Tests

```bash
# Run specific test
pytest tests/test_cli.py::TestCLI::test_basic_functionality -v

# Run with debugger
pytest tests/test_cli.py::TestCLI::test_basic_functionality --pdb

# Run with print statements
pytest tests/test_cli.py::TestCLI::test_basic_functionality -s
```

### Common Issues

1. **Import errors**: Check Python path and module structure
2. **Mock not working**: Verify the patch target path
3. **Fixtures not found**: Check fixture scope and location
4. **Tests hanging**: Check for infinite loops or missing mocks

### Test Output

```bash
# Increase verbosity
pytest -vv

# Show local variables on failure
pytest -l

# Show full traceback
pytest --tb=long

# Show only failed tests
pytest --failed-first
```

## Performance Testing

### Benchmark Tests

```python
import pytest

def test_migration_performance(benchmark):
    """Benchmark migration performance."""
    def migrate_data():
        # Migration code here
        pass
    
    result = benchmark(migrate_data)
    assert result is not None
```

### Profiling

```bash
# Profile test execution
pytest --profile

# Memory profiling
pytest --memray

# Time profiling
pytest --durations=10
```

## Maintenance

### Updating Tests

1. Run tests before making changes
2. Update tests when changing functionality
3. Add tests for new features
4. Remove tests for deleted features

### Test Health

```bash
# Check for unused fixtures
pytest --collect-only | grep "fixture"

# Find slow tests
pytest --durations=0

# Check test coverage gaps
pytest --cov=tools --cov-report=html
```

## Troubleshooting

### Common Problems

1. **Tests pass locally but fail in CI**
   - Check environment differences
   - Verify all dependencies are installed
   - Check for hardcoded paths

2. **Flaky tests**
   - Add proper waits for async operations
   - Use deterministic test data
   - Mock time-dependent functions

3. **Slow test suite**
   - Profile slow tests
   - Use mocks instead of real services
   - Run tests in parallel

### Getting Help

1. Check test output and error messages
2. Review similar tests for patterns
3. Check pytest documentation
4. Ask team members for help

---

Happy testing! 🧪✨