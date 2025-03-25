import yaml
from workflow_definition.job_helper.model.job_config import JobConfig

class JobLoader:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.job_config = None
        self.jobs = []

    def load_config(self):
        with open(self.config_path, 'r') as file:
            self.config = yaml.safe_load(file) 
        self.job_config = JobConfig(**self.config)

    def create_jobs(self):
        for layer in ['landing', 'bronze', 'silver', 'gold']:
            layer_config = getattr(self.job_config, layer, None)
            if layer_config:
                for job_layer in layer_config.values():
                    for task in job_layer.tasks.values():
                        task_details = {
                            'task_key': task.name,
                            'description': task.description,
                            'spark_python_task': {
                                'python_file': task.python_file,
                                'parameters': task.parameters
                            },
                            'depends_on': [{'task_key': dep} for dep in task.depends_on] if task.depends_on else []
                        }
                        self.jobs.append(task_details)