import os
import json
import argparse
import re
from typing import Any, Callable, Dict, List, Tuple
from workflow_definition.job_helper.model.job_config import JobConfig
from workflow_definition.job_helper.model.task import Task
from workflow_definition.job_helper.model.job_layer import JobLayer
from workflow_definition.job_helper.utils import get_current_branch, load_yaml_file, save_yaml_file


def separate_jobs_by_env(job_names: List[str], ws_env: str) -> List[str]:
    return [job_name for job_name in job_names if job_name.startswith(f'{ws_env}_')]
    
def build_task_params(task: dict[str, Task]) -> dict[str, Any]:
    """
    Builds task parameters based on the provided task configuration.

    This function constructs a dictionary of task parameters, including job parameters
    and client settings, derived from the given task object.

    Args:
        task: The task object containing configuration details.

    Returns:
        dict[str, Any]: A dictionary containing the constructed task parameters.
        It includes:
        - 'job_run_id': Set to '{{job.run_id}}'
        - Job parameters from task.parameters['job_parameters']
        - Custom parameters from task.parameters['custom_parameters']
        - 'client_settings': A dictionary of task-specific settings

    Note:
        The function removes any keys with None values from the client_settings
        dictionary to clean up the final output.
    """

    task_job_params = {
        'job_run_id': '{{job.run_id}}', # https://docs.databricks.com/en/workflows/jobs/parameter-value-references.html#supported-value-references
    }
    
    for param_identifier, value in task.parameters.items():
        if param_identifier == 'job_parameters':
            for param in value:
                task_job_params[param] = f'{{{{job.parameters.`{param}`}}}}'
                
        elif param_identifier == 'custom_parameters':
            for key, param in value.items():
                task_job_params[key] = param
    
    task_params = {
        "client_settings": {
            "description": task.description,
            "compression": task.compression,
            "file_type": task.format,
            "file_pattern": task.file_pattern.dict() if task.file_pattern else None,
            "file_parser": task.file_parser.dict() if task.file_parser else None,
            "output_table_name": task.output_table_name
        }
    }

    task_params.update(task_job_params)

    # Remove keys with None values to clean up the dictionary
    task_params["client_settings"] = {k: v for k, v in task_params["client_settings"].items() if v is not None}

    return task_params

def orchestrate_tasks(
    tasks_info: dict[str, Task],
    cluster_config: dict[str, Any],
    libs_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Orchestrates bronze tasks based on the provided settings and configurations.

    Args:
        job_config (JobConfig): Configuration for the jobs.
        cluster_config (dict[str, Any]): Configuration for the cluster.
        libs_config (dict[str, Any]): Configuration for libraries.

    Returns:
        list[dict[str, Any]]: A list of task definitions for the bronze tasks.
    """

    job_tasks = []

    for task in tasks_info.values():
        task_params = build_task_params(task)
        task_key = f'{task.name}'
        task_details = {
            'task_key': task_key,
            'description': task.description,
            'job_cluster_key': cluster_config['job_cluster_key'],
            'spark_python_task': {
                'python_file': task.python_file,
                'parameters': [json.dumps(task_params)]
            },
            'libraries': libs_config['libraries'],
            'depends_on': [{'task_key': dep} for dep in task.depends_on] if task.depends_on else []
        }
        job_tasks.append(task_details)

    return job_tasks

def build_layer_job(
    ws_env: str,
    layer: str,
    customer_code: str,
    industry: str,
    layer_info: dict,
    cluster_config: dict[str, Any],
    libs_config: dict[str, Any],
    branch_name: str,
    orchestrator: Callable[[JobConfig, Dict[str, Any], Dict[str, Any]], List[Dict[str, Any]]]
) -> Tuple[str, Dict[str, Any]]:
    
    client_job_key = f'{ws_env}_deus_{industry}_{customer_code}_{layer}_job_{branch_name}_branch'

    job_layer = next(iter(layer_info.values()))
    job_parameters = job_layer.job_parameters
    job_parameters = [{'name': name, 'default': default} for name, default in job_parameters.items()] if job_parameters else []
    
    # Add ws_env parameter if not present
    if not any(param['name'] == 'ws_env' for param in job_parameters):
        job_parameters.append({'name': 'ws_env', 'default': ws_env})

    client_job_content = {
        'name': client_job_key,
        'parameters': job_parameters,
        'job_clusters': [cluster_config],
        'email_notifications': {'on_failure': '${workspace.current_user.userName}'}
    }
    
    tasks_info = job_layer.tasks

    tasks = orchestrator(tasks_info, cluster_config, libs_config)
    client_job_content['tasks'] = tasks

    return (client_job_key, client_job_content)

def main(ws_env: str = None, customer_code: str = None, resource_dir=None):
    """
    Main function to load configurations, prepare client jobs, and save them into a YAML file as resources.

    This function orchestrates the creation of jobs for various clients based on their configurations.
    It loads shared configurations for clusters and libraries, reads client-specific job configurations,
    builds the required jobs for each client, and finally saves all jobs into a consolidated YAML file.

    Returns:
        None
    """
    branch_name = get_current_branch()
    
    if ws_env == "prod" and branch_name != "main":
        print("Error, can't deploy into PROD any other branch than main")
        return 0
        
    if resource_dir is None:
        current_file_directory = os.path.dirname(os.path.abspath(__file__)).replace('/job_helper', '')
        project_root = os.path.dirname(current_file_directory)
    else:
        project_root = resource_dir
    
    # build file path for cluster, lib config and client jobs
    cluster_config_file_path = os.path.join(project_root, "workflow_definition/shared/job_cluster.yml")
    libs_config_file_path = os.path.join(project_root, "workflow_definition/shared/job_libraries.yml")
    clients_jobs_dir = os.path.join(project_root, "workflow_definition/clients_jobs")
    
    # load cluster and libs config
    cluster_config = load_yaml_file(file_path=cluster_config_file_path)
    libs_config = load_yaml_file(file_path=libs_config_file_path)
    
    # build client_job_path definition
    filtered_client = str(customer_code).lower() if customer_code is not None else ''
    yaml_suffix = f'{filtered_client}.yml'
    client_jobs_file_yaml_paths = [os.path.join(clients_jobs_dir, client_job_filename) for client_job_filename in os.listdir(clients_jobs_dir) if client_job_filename.endswith(yaml_suffix)]
        
    client_jobs = {}
    
    for client_job_path in client_jobs_file_yaml_paths:
        client_settings = load_yaml_file(file_path=client_job_path)
        
        if client_settings:
            customer_code = str(client_settings['customer_code']).upper()
            industry = client_settings['industry']

            job_config = JobConfig(**client_settings)

            for layer in ['landing', 'bronze', 'silver', 'gold']:
                layer_info = getattr(job_config, layer, None)

                if layer_info is not None:
                    job_key, job = build_layer_job(
                        ws_env=ws_env,
                        layer=layer,
                        customer_code=customer_code,
                        industry=industry,
                        layer_info=layer_info,
                        cluster_config=cluster_config,
                        libs_config=libs_config,
                        branch_name=branch_name,
                        orchestrator=orchestrate_tasks
                    )
                    client_jobs[job_key] = job
    
    # Separar los trabajos por entorno (aunque ahora todos los jobs generados serán del entorno correcto)
    env_jobs = separate_jobs_by_env(client_jobs.keys(), ws_env)
    
    all_jobs = {'resources': {'jobs': {}}}
    
    for key in env_jobs:
        all_jobs['resources']['jobs'][key] = client_jobs[key]
    
    yaml_file_path = os.path.join(project_root, 'resources/jobs.yml')
    save_yaml_file(filepath=yaml_file_path, content=all_jobs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Pack databricks inner job for workspace.")
    parser.add_argument("--target", required=True, help="Specify the target environment")
    parser.add_argument("--customer", required=False) # optional
    args = parser.parse_args()
    ws_env = args.target # workspace environment
    customer_code = args.customer
    main(ws_env=ws_env, customer_code=customer_code) 
