# Refactoring Documentation

## Overview
This document tracks the refactoring of the codebase to make it more generic. The main terminology changes (`industry` to `domain` and `customer_code` to `dds_code`) have already been implemented in core files.

## Current Status
✅ Core terminology changes completed in:
- workflow_definition/job_helper/model/job_config.py
- tests/test_job_config.py
- tests/test_job_layer.py
- tests/test_job_loader.py
- tests/test_merge_jobs_main.py
- deus_lib/utils/pydantic_validation.py
- deus_lib/bronze_factory/bronze_pydantic_validation.py
- deus_lib/silver_factory/silver_pydantic_validation.py
- src/tasks/dds/common_behaviours.py
- src/tasks/dds/common_merge.py

## Remaining Changes

### Phase 1: Template and Configuration Updates
✅ Update test data templates in tests/test_data/
  ✅ Review and update valid_etl-settings.template.yml
  ✅ Update custom_task_etl-settings.vs.yml with new terminology
  ✅ Verify valid_etl-settings.vs.yml terminology
  ✅ Verify landing_job_config.yml terminology
✅ Update example configurations in docs/
  ✅ No example configurations found in docs/
  ✅ Added example configurations for reference

### Phase 2: Validation and Utils Updates
✅ Review and update deus_lib/utils/pydantic_validation.py
  ✅ Updated extract_client_names to extract_dds_codes
  ✅ Updated error messages and docstrings
  ✅ Verified validation schemas using new terminology
✅ Update factory validations
  ✅ Updated bronze_factory/bronze_pydantic_validation.py (ClientSettings → DataSettings)
  ✅ Verified silver_factory/silver_pydantic_validation.py (already using new terminology)
  ✅ Updated validation error messages

### Phase 3: DDS Job Updates
✅ Review src/tasks/clients/
  ✅ Renamed directory to src/tasks/dds/
  ✅ Updated common_behaviours.py with new terminology
  ✅ Updated common_merge.py with new terminology
  ✅ Renamed import_client_module to import_dds_module
  ✅ Updated function parameters and docstrings
✅ Improve error handling and logging
  ✅ Added comprehensive docstrings to all methods
  ✅ Implemented consistent error handling patterns
  ✅ Enhanced logging with appropriate log levels
  ✅ Added detailed error messages with context
  ✅ Standardized exception handling across behaviours
✅ Update DDS-specific job configurations
  ✅ Review and update each DDS code's mapping.json
  ✅ Update DDS-specific task parameters
  ✅ Update DDS-specific documentation

### Phase 4: Documentation Updates
✅ Update README.md with new terminology
✅ Update API documentation
✅ Update developer guides
✅ Update configuration guides

### Phase 5: Testing and Validation
✅ Run full test suite
- [ ] Verify all job configurations still work
- [ ] Check all validation messages
- [ ] Test with sample client data
- [ ] Perform integration tests

## Progress Log

### April 2024
- Core terminology changes (`industry` to `domain`, `customer_code` to `dds_code`) completed in main model and test files
- Security updates merged into main branch
- Test suite passing with updated terminology
- Updated all test data templates with new terminology:
  - Verified valid_etl-settings.template.yml
  - Updated custom_task_etl-settings.vs.yml
  - Verified valid_etl-settings.vs.yml
  - Verified landing_job_config.yml
- Noted absence of example configurations in docs/ directory
- Updated validation files with new terminology:
  - Renamed extract_client_names to extract_dds_codes in pydantic_validation.py
  - Renamed ClientSettings to DataSettings in bronze_factory
  - Verified silver_factory already using new terminology
- Updated client jobs to use new terminology:
  - Renamed src/tasks/clients/ to src/tasks/dds/
  - Updated common_behaviours.py and common_merge.py
  - Renamed import_client_module to import_dds_module
  - Updated function parameters and docstrings

### 2024-04-02
- Updated all mapping files to use new terminology:
  - Replaced `customer_code` with `dds_code` in field names
  - Replaced `industry` with `domain` in configuration
  - Updated catalog paths from `client-*` to `dds-*`
  - Updated `customerCode` to `ddsCode` in ST mapping
- Renamed `clients` directory to `dds` to reflect new terminology
- Updated common files to use new terminology:
  - Modified `common_behaviours.py` to use `dds_code`
  - Updated `common_merge.py` to use new terminology
- All changes have been committed and pushed to the main branch
- Improved error handling and logging in BaseCalculatorBehaviours:
  - Added comprehensive docstrings to all methods
  - Implemented consistent error handling patterns
  - Enhanced logging with appropriate log levels
  - Added detailed error messages with context
  - Standardized exception handling across behaviours

### 2024-04-03
- Updated DDS-specific task parameters:
  - Changed `customer_code` to `dds_code` in VS CTAS tables
  - Moved hardcoded vessel configurations to `vessel_config.json`
  - Made vessel names generic in ST preprocessing
- Added comprehensive DDS module documentation:
  - Created README.md in DDS directory
  - Documented standardized terminology
  - Documented directory structure and configuration files
  - Added best practices and guidelines
- Updated project documentation:
  - Updated main README.md with new terminology
  - Created API documentation (api.md)
  - Created developer guide (developer_guide.md)
  - Created configuration guide (config_guide.md)
- Ran full test suite successfully with all 20 tests passing
- Completed Phase 3 and Phase 4 of the refactoring plan

## Next Steps
1. Continue with Phase 5: Testing and Validation
   - Verify all job configurations still work
   - Check all validation messages
   - Test with sample client data
   - Perform integration tests

## Notes
- All new code should use the new terminology (`domain`, `dds_code`)
- Keep backward compatibility where possible
- Document any breaking changes in CHANGELOG.md 