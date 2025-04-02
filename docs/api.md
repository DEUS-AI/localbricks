# API Documentation

## Overview

This document describes the key APIs and interfaces used in the project, following the standardized terminology and patterns.

## Core APIs

### DDS Module (`src/tasks/dds/`)

#### Common Behaviors (`common_behaviours.py`)

```python
def import_dds_module(dds_code: str, module_name: str) -> None:
    """Import a module from the DDS code's directory.
    
    Args:
        dds_code (str): The DDS code to import from (e.g., 'vs', 'st')
        module_name (str): The name of the module to import
    """
```

```python
def calculate_behaviour(
    domain: str,
    dds_code: str,
    behaviour_name: str,
    data: DataFrame
) -> DataFrame:
    """Calculate a specific behavior for a given domain and DDS code.
    
    Args:
        domain (str): Industry domain (e.g., 'aviation', 'maritime')
        dds_code (str): DDS code identifier
        behaviour_name (str): Name of the behavior to calculate
        data (DataFrame): Input data for calculations
        
    Returns:
        DataFrame: Data with calculated behavior metrics
    """
```

#### Common Merge (`common_merge.py`)

```python
def merge_data_frames(
    domain: str,
    dds_code: str,
    data_frames: List[str],
    join_keys: List[str]
) -> DataFrame:
    """Merge multiple data frames based on join keys.
    
    Args:
        domain (str): Industry domain
        dds_code (str): DDS code identifier
        data_frames (List[str]): List of data frame names to merge
        join_keys (List[str]): Keys to join the data frames on
        
    Returns:
        DataFrame: Merged data frame
    """
```

### Configuration Interfaces

#### Mapping Configuration

The `mapping.json` file in each DDS code directory follows this structure:

```json
{
    "dataMappings": {
        "domain": "string",  // Industry domain
        "catalog": "string", // Format: "dds-{code}"
        "data_frames": ["string"],
        "join_keys": ["string"],
        "calculableBehaviours": ["string"],
        "csvFields": [
            {
                "name": "string",
                "type": "string",
                "options": ["string"],
                "eventType": "string"
            }
        ]
    }
}
```

#### Job Configuration

Job configuration YAML files in `workflow_definition/dds_jobs/` follow this structure:

```yaml
name: string
domain: string  # Industry domain
dds_code: string  # DDS code identifier
parameters:
  input_path: string
  output_path: string
dependencies:
  - task_id: string
    parameters: {}
```

## Error Handling

All modules implement standardized error handling:

```python
class DDSError(Exception):
    """Base exception for DDS-related errors."""
    pass

class DDSConfigError(DDSError):
    """Configuration-related errors."""
    pass

class DDSProcessingError(DDSError):
    """Data processing errors."""
    pass
```

## Logging

Standardized logging patterns:

```python
logger.info(f"Processing data for DDS code: {dds_code}")
logger.debug(f"Merging data frames: {data_frames}")
logger.error(f"Failed to process {dds_code} data: {error}")
```

## Best Practices

1. Always use the standardized terminology:
   - `domain` instead of `industry`
   - `dds_code` instead of `customer_code`
   - `dds_name` instead of `customer_name`

2. Follow the catalog naming convention:
   - Use `dds-{code}` format
   - Example: `dds-vs`, `dds-st`

3. Implement proper error handling:
   - Use custom exception classes
   - Include context in error messages
   - Log appropriate information

4. Use type hints and docstrings:
   - Document all parameters
   - Specify return types
   - Include usage examples

## Migration Guide

When migrating existing code:

1. Update parameter names:
   ```python
   # Old
   def process_data(customer_code: str, industry: str)
   
   # New
   def process_data(dds_code: str, domain: str)
   ```

2. Update configuration keys:
   ```json
   // Old
   {
     "industry": "aviation",
     "customer_code": "vs"
   }
   
   // New
   {
     "domain": "aviation",
     "dds_code": "vs"
   }
   ```

3. Update catalog references:
   ```python
   # Old
   catalog = f"client-{code}"
   
   # New
   catalog = f"dds-{code}"
   ``` 