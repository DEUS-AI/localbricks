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

def test_orchestrate_landing_tasks():
    """Test orchestration of landing tasks with generic configurations."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    cluster_config = load_yaml('tests/test_data/job_cluster.yml')
    libs_config = load_yaml('tests/test_data/job_libraries.yml')
    
    tasks = orchestrate_tasks(job_config.landing["ingestion"].tasks, cluster_config, libs_config)
    assert len(tasks) > 0
    assert tasks[0]["task_key"] == "raw_data_task"

def test_orchestrate_bronze_tasks():
    """Test orchestration of bronze tasks with generic configurations."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    cluster_config = load_yaml('tests/test_data/job_cluster.yml')
    libs_config = load_yaml('tests/test_data/job_libraries.yml')
    
    tasks = orchestrate_tasks(job_config.bronze["processing"].tasks, cluster_config, libs_config)
    assert len(tasks) > 0
    assert tasks[0]["task_key"] == "data_source_1_task"

def test_build_layer_job_landing():
    """Test building landing layer job with generic configurations."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    cluster_config = load_yaml('tests/test_data/job_cluster.yml')
    libs_config = load_yaml('tests/test_data/job_libraries.yml')
    
    job_key, job = build_layer_job(
        ws_env="dev",
        layer="landing",
        dds_code="DDS_CODE",
        domain="DOMAIN_TYPE",
        layer_info=job_config.landing,
        cluster_config=cluster_config,
        libs_config=libs_config,
        branch_name="main",
        orchestrator=orchestrate_tasks
    )
    
    assert job_key == "dev_deus_DOMAIN_TYPE_DDS_CODE_landing_job_main_branch"
    assert job["name"] == job_key
    assert len(job["tasks"]) > 0

def test_build_layer_job_bronze():
    """Test building bronze layer job with generic configurations."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    cluster_config = load_yaml('tests/test_data/job_cluster.yml')
    libs_config = load_yaml('tests/test_data/job_libraries.yml')
    
    job_key, job = build_layer_job(
        ws_env="dev",
        layer="bronze",
        dds_code="DDS_CODE",
        domain="DOMAIN_TYPE",
        layer_info=job_config.bronze,
        cluster_config=cluster_config,
        libs_config=libs_config,
        branch_name="main",
        orchestrator=orchestrate_tasks
    )
    
    assert job_key == "dev_deus_DOMAIN_TYPE_DDS_CODE_bronze_job_main_branch"
    assert job["name"] == job_key
    assert len(job["tasks"]) > 0

def test_job_naming_convention():
    """Test that job naming follows the new convention."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    cluster_config = load_yaml('tests/test_data/job_cluster.yml')
    libs_config = load_yaml('tests/test_data/job_libraries.yml')
    
    for layer in ["landing", "bronze"]:
        layer_info = getattr(job_config, layer)
        if layer_info:
            job_key, _ = build_layer_job(
                ws_env="dev",
                layer=layer,
                dds_code="DDS_CODE",
                domain="DOMAIN_TYPE",
                layer_info=layer_info,
                cluster_config=cluster_config,
                libs_config=libs_config,
                branch_name="main",
                orchestrator=orchestrate_tasks
            )
            assert job_key.startswith("dev_deus_")
            assert "DDS_CODE" in job_key
            assert "DOMAIN_TYPE" in job_key

if __name__ == "__main__":
    pytest.main()