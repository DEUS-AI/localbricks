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

## Remaining Changes

### Phase 1: Template and Configuration Updates
✅ Update test data templates in tests/test_data/
  ✅ Review and update valid_etl-settings.template.yml
  ✅ Update custom_task_etl-settings.vs.yml with new terminology
  ✅ Verify valid_etl-settings.vs.yml terminology
  ✅ Verify landing_job_config.yml terminology
- [ ] Update example configurations in docs/
  ✅ No example configurations found in docs/
  - [ ] Consider adding example configurations for reference

### Phase 2: Validation and Utils Updates
✅ Review and update deus_lib/utils/pydantic_validation.py
  ✅ Updated extract_client_names to extract_dds_codes
  ✅ Updated error messages and docstrings
  ✅ Verified validation schemas using new terminology
✅ Update factory validations
  ✅ Updated bronze_factory/bronze_pydantic_validation.py (ClientSettings → DataSettings)
  ✅ Verified silver_factory/silver_pydantic_validation.py (already using new terminology)
  ✅ Updated validation error messages

### Phase 3: Client Job Updates
- [ ] Review workflow_definition/clients_jobs/
  - [ ] Update any client-specific job configurations
  - [ ] Update client-specific task parameters
  - [ ] Update client documentation

### Phase 4: Documentation Updates
- [ ] Update README.md with new terminology
- [ ] Update API documentation
- [ ] Update developer guides
- [ ] Update configuration guides

### Phase 5: Testing and Validation
- [ ] Run full test suite
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

## Next Steps
1. Move on to Phase 3: Client Job Updates
2. Review client-specific job configurations
3. Update client-specific task parameters
4. Update client documentation

## Notes
- All new code should use the new terminology (`domain`, `dds_code`)
- Keep backward compatibility where possible
- Document any breaking changes in CHANGELOG.md 