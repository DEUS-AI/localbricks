# Refactoring Documentation

## Overview
This document tracks the refactoring of the codebase to make it more generic by:
- Changing `industry` to `domain`
- Changing `customer_code` to `dds_code` (Domain Data Store code)

## Changes Tracking

### Phase 1: Test Updates
- [ ] Update test configuration files
- [ ] Update test assertions
- [ ] Update test data templates

### Phase 2: Core Code Updates
- [ ] Update model definitions
- [ ] Update validation logic
- [ ] Update job configuration handling
- [ ] Update task parameters

### Phase 3: Validation
- [ ] Run all tests
- [ ] Fix any failing tests
- [ ] Perform sanity check on src code

## Detailed Changes

### Test Files to Update
1. tests/test_job_config.py
2. tests/test_merge_jobs_main.py
3. tests/test_job_layer.py
4. tests/test_job_loader.py

### Core Files to Update
1. workflow_definition/job_helper/model/job_config.py
2. deus_lib/utils/pydantic_validation.py
3. workflow_definition/job_helper/merge_jobs.py
4. deus_lib/bronze_factory/bronze_pydantic_validation.py
5. deus_lib/silver_factory/silver_pydantic_validation.py

## Progress Log
(Changes will be logged here as they are made) 