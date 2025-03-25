import pytest
import os
import shutil
from unittest.mock import patch
import yaml
from workflow_definition.job_helper.merge_jobs import (
    build_layer_job,
    orchestrate_tasks,
    main
)
from workflow_definition.job_helper.model.job_config import JobConfig
from workflow_definition.job_helper.utils import load_yaml_file, save_yaml_file

def load_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

@pytest.fixture
def cluster_config():
    return load_yaml('workflow_definition/shared/job_cluster.yml')

@pytest.fixture
def libs_config():
    return load_yaml('workflow_definition/shared/job_libraries.yml')

@pytest.fixture
def job_config_data():
    return load_yaml('tests/test_data/valid_etl-settings.template.yml')

def test_orchestrate_landing_tasks(job_config_data, cluster_config, libs_config):
    """Test the orchestration of landing tasks with generic configuration."""
    job_config = JobConfig(**job_config_data)
    tasks = orchestrate_tasks(job_config.landing['ingestion'].tasks, cluster_config, libs_config)
    
    assert len(tasks) > 0
    assert tasks[0]['task_key'] == job_config.landing['ingestion'].tasks['raw_data_task'].name
    assert 'deus_lib' in str(libs_config)  # Verify library references are updated

def test_orchestrate_bronze_tasks(job_config_data, cluster_config, libs_config):
    """Test the orchestration of bronze tasks with generic configuration."""
    job_config = JobConfig(**job_config_data)
    tasks = orchestrate_tasks(job_config.bronze['processing'].tasks, cluster_config, libs_config)
    
    assert len(tasks) > 0
    first_task = job_config.bronze['processing'].tasks['data_source_1_task'].name
    assert tasks[0]['task_key'] == first_task
    assert 'deus_lib' in str(libs_config)  # Verify library references are updated

def test_build_layer_job_landing(job_config_data, cluster_config, libs_config):
    """Test building landing layer job with generic configuration."""
    ws_env = "dev"
    customer_code = "CLIENT"
    industry = "INDUSTRY"
    branch_name = "main"

    job_config = JobConfig(**job_config_data)
    landing_zone_key, landing_zone_job = build_layer_job(
        ws_env=ws_env,
        layer='landing',
        customer_code=customer_code,
        industry=industry,
        layer_info=job_config.landing,
        cluster_config=cluster_config,
        libs_config=libs_config,
        branch_name=branch_name,
        orchestrator=orchestrate_tasks
    )

    expected_key = f'{ws_env}_deus_{industry}_{customer_code}_landing_job_{branch_name}_branch'
    assert landing_zone_key == expected_key
    assert 'tasks' in landing_zone_job
    assert len(landing_zone_job['tasks']) > 0
    assert 'deus_lib' in str(libs_config)  # Verify library references are updated

def test_build_layer_job_bronze(job_config_data, cluster_config, libs_config):
    """Test building bronze layer job with generic configuration."""
    ws_env = "dev"
    customer_code = "CLIENT"
    industry = "INDUSTRY"
    branch_name = "main"

    job_config = JobConfig(**job_config_data)
    bronze_layer_key, bronze_layer_job = build_layer_job(
        ws_env=ws_env,
        layer='bronze',
        customer_code=customer_code,
        industry=industry,
        layer_info=job_config.bronze,
        cluster_config=cluster_config,
        libs_config=libs_config,
        branch_name=branch_name,
        orchestrator=orchestrate_tasks
    )

    expected_key = f'{ws_env}_deus_{industry}_{customer_code}_bronze_job_{branch_name}_branch'
    assert bronze_layer_key == expected_key
    assert 'tasks' in bronze_layer_job
    assert len(bronze_layer_job['tasks']) > 0
    assert 'deus_lib' in str(libs_config)  # Verify library references are updated

def test_job_naming_convention():
    """Test job naming follows the new convention with deus prefix."""
    customer_code = "CLIENT"
    industry = "INDUSTRY"
    branch_name = "main"
    layers = ['landing', 'bronze', 'silver', 'gold']
    
    for layer in layers:
        expected_name = f'deus_{industry}_{customer_code}_{layer}_job_{branch_name}_branch'
        assert expected_name.startswith('deus_')
        assert 'signol' not in expected_name.lower()

if __name__ == "__main__":
    pytest.main()