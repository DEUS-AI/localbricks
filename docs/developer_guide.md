# Developer Guide

## Introduction

This guide provides detailed information for developers working on the project, including setup instructions, coding standards, and best practices.

## Getting Started

1. Clone the repository
2. Install dependencies using Poetry:
   ```bash
   poetry install
   ```
3. Set up Databricks CLI configuration
4. Review the API documentation in `docs/api.md`

## Project Structure

```
.
├── deus_lib/              # Core library
│   ├── utils/            # Utility functions
│   ├── bronze_factory/   # Bronze layer processing
│   └── silver_factory/   # Silver layer processing
├── src/
│   └── tasks/
│       └── dds/         # DDS-specific tasks
│           ├── common_behaviours.py
│           ├── common_merge.py
│           └── {dds_code}/  # DDS code specific implementations
├── tests/               # Test suites
└── workflow_definition/ # Job definitions
```

## Adding a New DDS Code

1. Create a new directory in `src/tasks/dds/{dds_code}/`

2. Create required configuration files:
   ```
   {dds_code}/
   ├── mapping.json      # Field mappings and configurations
   ├── preprocess.py     # Data preprocessing logic
   └── behaviours.py     # Custom behavior implementations
   ```

3. Define the mapping.json:
   ```json
   {
       "dataMappings": {
           "domain": "aviation|maritime",
           "catalog": "dds-{code}",
           "data_frames": [],
           "join_keys": [],
           "calculableBehaviours": []
       }
   }
   ```

4. Implement preprocessing if needed:
   ```python
   def preprocess_data(data: DataFrame) -> DataFrame:
       """Preprocess data for the specific DDS code."""
       # Implementation
       return processed_data
   ```

5. Add job configuration in `workflow_definition/dds_jobs/`

6. Add tests in `tests/`

## Development Workflow

1. Create a new branch for your changes
2. Make code changes following the standards
3. Run tests: `pytest tests/`
4. Update documentation if needed
5. Submit a pull request

## Coding Standards

### Naming Conventions

- Use `domain` instead of `industry`
- Use `dds_code` instead of `customer_code`
- Use `dds_name` instead of `customer_name`
- Use `dds-{code}` format for catalog paths

### Type Hints

Always use type hints in function definitions:

```python
def process_data(
    dds_code: str,
    domain: str,
    data: DataFrame
) -> DataFrame:
    """Process data for a specific DDS code.
    
    Args:
        dds_code: DDS code identifier
        domain: Industry domain
        data: Input DataFrame
        
    Returns:
        Processed DataFrame
    """
    pass
```

### Error Handling

Use custom exception classes and proper error messages:

```python
try:
    process_data(dds_code, domain, data)
except DDSConfigError as e:
    logger.error(f"Configuration error for {dds_code}: {e}")
    raise
except DDSProcessingError as e:
    logger.error(f"Processing error for {dds_code}: {e}")
    raise
```

### Logging

Use appropriate log levels:

```python
logger.debug("Detailed debugging information")
logger.info(f"Processing {dds_code} data")
logger.warning(f"Missing optional field in {dds_code}")
logger.error(f"Failed to process {dds_code}")
```

## Testing

### Unit Tests

Write tests for all new functionality:

```python
def test_process_data():
    """Test data processing for a specific DDS code."""
    data = create_test_data()
    result = process_data("vs", "aviation", data)
    assert_data_valid(result)
```

### Integration Tests

Test the complete workflow:

```python
def test_end_to_end():
    """Test complete data processing pipeline."""
    config = load_config()
    result = run_pipeline(config)
    verify_output(result)
```

## Deployment

1. Build the package:
   ```bash
   make build-wheel
   ```

2. Pack the jobs:
   ```bash
   make pack-jobs
   ```

3. Deploy to development:
   ```bash
   make ci-pipeline-dev
   ```

## Troubleshooting

Common issues and solutions:

1. Missing dependencies:
   ```bash
   poetry install
   ```

2. Configuration errors:
   - Check mapping.json format
   - Verify catalog paths
   - Validate domain values

3. Data processing errors:
   - Check input data schema
   - Verify join keys exist
   - Review preprocessing logic

## Resources

- API Documentation: `docs/api.md`
- Configuration Guide: `docs/config_guide.md`
- Test Data: `tests/test_data/` 