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

## Remaining Changes

### Phase 1: Template and Configuration Updates
- [ ] Update test data templates in tests/test_data/
  - [ ] Review and update valid_etl-settings.template.yml
  - [ ] Update any other template files
- [ ] Update example configurations in docs/
  - [ ] Review and update example job configurations
  - [ ] Update configuration documentation

### Phase 2: Validation and Utils Updates
- [ ] Review and update deus_lib/utils/pydantic_validation.py
  - [ ] Check for any remaining old terminology
  - [ ] Update validation schemas if needed
- [ ] Update factory validations
  - [ ] Review bronze_factory/bronze_pydantic_validation.py
  - [ ] Review silver_factory/silver_pydantic_validation.py
  - [ ] Update any validation error messages

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

## Next Steps
1. Begin with Phase 1: Template and Configuration Updates
2. Review client jobs for any remaining old terminology
3. Update documentation to reflect new terminology
4. Run comprehensive tests to ensure no regressions

## Notes
- All new code should use the new terminology (`domain`, `dds_code`)
- Keep backward compatibility where possible
- Document any breaking changes in CHANGELOG.md 