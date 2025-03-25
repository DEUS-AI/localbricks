import pytest
from pydantic import ValidationError

from workflow_definition.job_helper.jobs_loader import JobLoader
from workflow_definition.job_helper.task_loader import TaskLoader

def test_load_valid_config():
    job_loader = JobLoader(config_path='tests/test_data/valid_etl-settings.vs.yml')
    job_loader.load_config()
    
    assert job_loader.job_config.customer_code == "VS"
    assert job_loader.job_config.industry == "AVIATION"
    assert "ingestion"in job_loader.job_config.landing
    assert "processing" in job_loader.job_config.bronze

def test_load_invalid_config():
    job_loader = JobLoader(config_path='tests/test_data/invalid_etl-settings.vs.yml')
    
    with pytest.raises(ValidationError):
        job_loader.load_config()

def test_create_jobs():
    job_loader = JobLoader(config_path='tests/test_data/valid_etl-settings.vs.yml')
    job_loader.load_config()
    job_loader.create_jobs()
    
    assert len(job_loader.jobs) > 0
    assert any(job['task_key'] == "raw_data_task" for job in job_loader.jobs)

def test_load_custom_task():
    task_loader = TaskLoader(task_path='tests/test_data/custom_task.py')
    custom_task = task_loader.load_task()
    
    assert callable(custom_task.custom_task)

if __name__ == "__main__":
    pytest.main()
    