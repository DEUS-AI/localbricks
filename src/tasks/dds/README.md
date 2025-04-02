# DDS (Data Delivery Service) Module

This module contains the core functionality for processing data from different domains (aviation, maritime, etc.) using standardized terminology and structures.

## Directory Structure

```
dds/
├── common_behaviours.py   # Common behavior calculations shared across domains
├── common_merge.py        # Common data merging utilities
├── vs/                    # Aviation domain tasks
├── st/                    # Maritime domain tasks (Shipping)
├── chr/                   # Maritime domain tasks (Charter)
├── ldc/                   # Maritime domain tasks (LDC)
├── pga/                   # Maritime domain tasks (PGA)
└── cgl/                   # Maritime domain tasks (CGL)
```

## Terminology

The module uses standardized terminology across all domains:

- `domain`: The industry sector (e.g., "aviation", "maritime")
- `dds_code`: Unique identifier for a data source
- `dds_name`: Human-readable name for a data source

## Configuration Files

Each DDS code directory contains the following key files:

- `mapping.json`: Field mappings and data transformations
  - Defines field names, types, and options
  - Specifies catalog paths in format `dds-{code}`
  - Contains domain-specific behavior configurations
  
- `vessel_config.json` (ST only): Vessel-specific configurations
  - Engine parameters and thresholds
  - Moved from hardcoded values to configurable JSON

## Common Utilities

### common_behaviours.py
- Shared behavior calculations
- Domain-agnostic implementations
- Standardized parameter names

### common_merge.py
- Data merging utilities
- Standardized join operations
- Common preprocessing functions

## Adding New DDS Codes

1. Create a new directory with the DDS code
2. Create required configuration files:
   - mapping.json
   - Any domain-specific configs
3. Implement domain-specific preprocessing if needed
4. Update tests to cover new code

## Best Practices

1. Use standardized terminology in all new code
2. Keep domain-specific logic in respective directories
3. Move common functionality to shared modules
4. Document all configuration parameters
5. Follow naming conventions for consistency 