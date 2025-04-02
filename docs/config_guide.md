# Configuration Guide

## Overview

This guide explains how to configure the data processing pipeline for different domains and DDS codes.

## Configuration Files

### 1. Mapping Configuration (`mapping.json`)

Each DDS code requires a `mapping.json` file that defines data mappings and transformations.

#### Basic Structure

```json
{
    "dataMappings": {
        "domain": "aviation|maritime",
        "catalog": "dds-{code}",
        "data_frames": ["frame1", "frame2"],
        "join_keys": ["key1", "key2"],
        "calculableBehaviours": ["behaviour1", "behaviour2"],
        "csvFields": []
    }
}
```

#### Example (Aviation Domain)

```json
{
    "dataMappings": {
        "domain": "aviation",
        "catalog": "dds-vs",
        "data_frames": [
            "flight_data",
            "fuelburn_data"
        ],
        "join_keys": [
            "aircraft_reg_code",
            "ocurrence_datetime"
        ],
        "calculableBehaviours": [
            "Reduced_Engine_Taxi_Out",
            "Efficient_Flight"
        ],
        "csvFields": [
            {
                "name": "event_id",
                "type": "string",
                "options": [],
                "eventType": "*"
            },
            {
                "name": "dds_code",
                "type": "string",
                "options": [],
                "eventType": "*"
            }
        ]
    }
}
```

#### Example (Maritime Domain)

```json
{
    "dataMappings": {
        "domain": "maritime",
        "catalog": "dds-st",
        "data_frames": [
            "vessel_data",
            "voyage_data"
        ],
        "join_keys": [
            "imo",
            "report_date"
        ],
        "calculableBehaviours": [
            "Efficient_Auxiliary_Engine_Use",
            "Optimal_Trim"
        ],
        "csvFields": [
            {
                "name": "event_id",
                "type": "string",
                "options": [],
                "eventType": "*"
            },
            {
                "name": "dds_code",
                "type": "string",
                "options": [],
                "eventType": "*"
            }
        ]
    }
}
```

### 2. Job Configuration (YAML)

Job configurations define the processing pipeline for each DDS code.

#### Basic Structure

```yaml
name: job_name
domain: domain_name
dds_code: dds_code
parameters:
  input_path: path/to/input
  output_path: path/to/output
dependencies:
  - task_id: task1
    parameters: {}
```

#### Example Job Configuration

```yaml
name: vs_data_processing
domain: aviation
dds_code: vs
parameters:
  input_path: /data/input/vs
  output_path: /data/output/vs
  date_format: "%Y-%m-%d"
dependencies:
  - task_id: data_validation
    parameters:
      schema_path: /schemas/vs_schema.json
  - task_id: data_transformation
    parameters:
      mapping_path: /mappings/vs_mapping.json
```

### 3. Domain-Specific Configuration

Some domains may require additional configuration files.

#### Maritime Vessel Configuration (`vessel_config.json`)

```json
{
    "vessel_type_1": {
        "AE1": {
            "max_load": 1400,
            "min_load": 110,
            "const": 0.8
        },
        "aeu_lower_threshold": 0.80
    }
}
```

## Configuration Guidelines

### 1. Naming Conventions

- Use `domain` instead of `industry`
- Use `dds_code` instead of `customer_code`
- Use `dds_name` instead of `customer_name`
- Use `dds-{code}` format for catalog paths

### 2. Field Definitions

Required fields in mapping.json:
- `event_id`
- `dds_code`
- `dds_name`
- Domain-specific fields

### 3. Data Types

Supported data types:
- `string`
- `number`
- `date`
- `boolean`

### 4. Event Types

Common event types:
- `*` (all events)
- `FLIGHT` (aviation)
- `SHIPPING` (maritime)

## Validation

### 1. Schema Validation

All configurations are validated against JSON schemas:

```python
from pydantic import BaseModel

class MappingConfig(BaseModel):
    domain: str
    catalog: str
    data_frames: List[str]
    join_keys: List[str]
```

### 2. Data Validation

Input data is validated against the field definitions:

```python
def validate_data(data: DataFrame, mapping: Dict) -> bool:
    """Validate data against mapping configuration."""
    for field in mapping["csvFields"]:
        if not validate_field(data, field):
            return False
    return True
```

## Examples

### 1. Adding a New Field

```json
{
    "csvFields": [
        {
            "name": "new_metric",
            "type": "number",
            "options": ["metric_value"],
            "eventType": "FLIGHT"
        }
    ]
}
```

### 2. Adding a New Behavior

```json
{
    "calculableBehaviours": [
        "New_Behavior_Name"
    ],
    "filters": {
        "New_Behavior_Name": [
            {
                "field_name": "vehicle",
                "values": ["type1", "type2"]
            }
        ]
    }
}
```

### 3. Custom Join Logic

```json
{
    "join_keys": ["key1", "key2"],
    "join_conditions": {
        "frame1_frame2": {
            "type": "inner",
            "additional_keys": ["key3"]
        }
    }
}
```

## Troubleshooting

Common configuration issues and solutions:

1. Invalid catalog path:
   - Ensure using `dds-{code}` format
   - Check permissions

2. Missing required fields:
   - Verify all required fields are present
   - Check field types match schema

3. Join key errors:
   - Confirm keys exist in all frames
   - Check key data types match

## Best Practices

1. Use descriptive names for fields and behaviors
2. Document all custom configurations
3. Keep configurations DRY (Don't Repeat Yourself)
4. Version control all configuration files
5. Test configurations before deployment 