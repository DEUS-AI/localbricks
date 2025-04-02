import pytest
from pydantic import ValidationError
import yaml
from workflow_definition.job_helper.model.job_config import JobConfig
from workflow_definition.job_helper.utils import load_yaml

def load_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def test_job_config_valid():
    """Test valid job configuration with generic settings."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    assert job_config.dds_code == "DDS_CODE"
    assert job_config.domain == "DOMAIN_TYPE"
    assert job_config.landing["ingestion"].tasks["raw_data_task"].name == "raw_data_task"
    assert job_config.bronze["processing"].tasks["data_source_1_task"].name == "data_source_1_task"
    assert job_config.silver["analysis"].tasks["silver_task"].name == "silver_task"
    assert job_config.gold["reporting"].tasks["gold_task"].name == "gold_task"

def test_job_config_missing_dds_code():
    """Test validation error when DDS code is missing."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    del job_config_data['dds_code']
    
    with pytest.raises(ValidationError):
        JobConfig(**job_config_data)

def test_job_config_missing_domain():
    """Test validation error when domain is missing."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    del job_config_data['domain']
    
    with pytest.raises(ValidationError):
        JobConfig(**job_config_data)

def test_job_config_optional_fields():
    """Test configuration with only required fields."""
    job_config_data = {
        "dds_code": "DDS_CODE",
        "domain": "DOMAIN_TYPE",
        "landing": {
            "ingestion": {
                "tasks": {
                    "raw_data_task": {
                        "name": "raw_data_task",
                        "description": "Generic data ingestion"
                    }
                }
            }
        }
    }

    job_config = JobConfig(**job_config_data)
    assert job_config.dds_code == "DDS_CODE"
    assert job_config.domain == "DOMAIN_TYPE"
    assert job_config.landing["ingestion"].tasks["raw_data_task"].name == "raw_data_task"
    assert job_config.bronze is None
    assert job_config.silver is None
    assert job_config.gold is None

def test_job_config_task_dependencies():
    """Test task dependencies configuration."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    # Check data expectations task dependencies
    data_exp_task = job_config.bronze["processing"].tasks["data_expectations_task"]
    assert "data_source_1_task" in data_exp_task.depends_on
    assert "data_source_2_task" in data_exp_task.depends_on

def test_job_config_file_patterns():
    """Test file pattern configurations."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    # Check landing task file pattern
    landing_task = job_config.landing["ingestion"].tasks["raw_data_task"]
    assert landing_task.file_pattern.name_regex == ".*\\\\.(csv|json)$"
    assert landing_task.file_pattern.min_size_bytes == 1
    assert landing_task.file_pattern.format is None
    
    # Check bronze task file pattern
    bronze_task = job_config.bronze["processing"].tasks["data_source_1_task"]
    assert bronze_task.file_pattern.name_regex == "^source1_.*\\\\.csv$"
    assert bronze_task.file_pattern.name == "source1"

def test_job_config_parameters():
    """Test task parameters configuration."""
    job_config_data = load_yaml('tests/test_data/valid_etl-settings.template.yml')
    job_config = JobConfig(**job_config_data)
    
    # Check silver task parameters
    silver_task = job_config.silver["analysis"].tasks["silver_task"]
    assert silver_task.parameters.get("start_date") == "value1"
    assert silver_task.parameters.get("end_date") == "value2"
    
    # Check bronze task parameters
    bronze_task = job_config.bronze["processing"].tasks["data_source_1_task"]
    assert bronze_task.file_parser.args["header"] == "true"
    assert bronze_task.file_parser.args["skipRows"] == 0

if __name__ == "__main__":
    pytest.main()