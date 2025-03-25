import json
from copy import deepcopy
from typing import Any, Optional

class Task:
    """
    Represents a task to be executed in a job workflow.

    Attributes:
        task_key (str): The unique key for the task.
        params (Optional[dict[str, Any]]): Parameters for the task. Defaults to an empty list.
        depends_on (Optional[list[dict[str, str]]]): List of dependencies for the task. Defaults to an empty list.
    """

    def __init__(self, task_key: str, params: Optional[dict[str, Any]] = None, depends_on: Optional[list[dict[str, str]]] = None):
        """
        Initializes a Task instance.

        Args:
            task_key (str): The unique key for the task.
            params (Optional[dict[str, Any]]): Parameters for the task. Defaults to None.
            depends_on (Optional[list[dict[str, str]]]): List of dependencies for the task. Defaults to None.
        """
        self.task_key = task_key
        self.params = params or []
        self.depends_on = depends_on or []

    def create_task_definition(self, python_file: str, cluster_config: dict[str, Any], libs_config: dict[str, Any]) -> dict[str, Any]:
        """
        Creates the task definition for a Spark Python task.

        Args:
            python_file (str): The path to the Python file to be executed.
            cluster_config (dict[str, Any]): The configuration for the job cluster.
            libs_config (dict[str, Any]): The configuration for the libraries.

        Returns:
            dict[str, Any]: The task definition dictionary.
        """

        return {
            "task_key": self.task_key,
            "spark_python_task": {
                "python_file": python_file,
                "parameters": [json.dumps(self.params)],
            },
            'job_cluster_key': cluster_config['job_cluster_key'],
                'libraries': deepcopy(libs_config['libraries']),
                'depends_on': self.depends_on
            }
    
    def create_job_runner_task(self, job_id: int) -> dict[str, Any]:
        """
        Creates the task definition for running a job task.

        Args:
            job_id (int): The ID of the job to be run.

        Returns:
            dict[str, Any]: The task definition dictionary.
        """
         
        return {
            "task_key": self.task_key,
            "run_job_task": {
                "job_id": job_id,
                "job_parameters": [json.dumps(self.params)] if len(self.params) != 0 else self.params,
            },
            'depends_on' : self.depends_on
            }