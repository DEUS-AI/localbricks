import subprocess, re, os
import argparse
from task_builder import Task
from utils import save_yaml_file, load_yaml_file, get_current_branch
from typing import Dict, List, Optional, Any, Tuple

def separate_jobs_by_env(job_names: List[str], ws_env: str) -> List[str]:
    return [job_name for job_name in job_names if job_name.startswith(f'{ws_env}_')]
    
def capture_job_ids(input_string: str, ws_env: str) -> Dict[str, List[str]]:
    job_ids_by_client: Dict[str, List[str]] = {}
    
    pattern: re.Pattern = re.compile(r'^(\d+)\s+(?:\[([^]]*)\]\s+)?(qa|prod)_deus_([^_]+)_([^_]+)_([^_]+)_job_([^_]+)_branch')

    lines: List[str] = input_string.split('\n')
    
    for line in lines:
        match: Optional[re.Match] = pattern.search(line)
        if match:
            job_id: str = match.group(1)
            developer: Optional[str] = match.group(2)
            job_env: str = match.group(3)
            industry: str = match.group(4)
            client_name: str = match.group(5)
            layer: str = match.group(6)
            branch: str = match.group(7)

            # Incluir el job si corresponde al entorno especificado
            if job_env == ws_env:
                job_name_id: str = f"{layer}_{job_id}"
                
                key: str = f"{industry}_{client_name}"
                if developer:
                    key = f"dev_{developer.split()[1]}_{key}"
                
                if key not in job_ids_by_client:
                    job_ids_by_client[key] = []
                    
                job_ids_by_client[key].append(job_name_id)
    
    return job_ids_by_client

def main(ws_env: str = None, customer_code: str = None):
    """
    Main function to capture Databricks jobs, process them, and update the resources YAML file with a outer job that is composed of these jobs.

    Returns:
        None
    """
    
    # capture jobs info (job name and id)
    result: subprocess.CompletedProcess = subprocess.run(['databricks', 'jobs', 'list'], capture_output=True, text=True)

    current_branch: str = get_current_branch()
    
    filtered_client_jobs = [str(customer_code).lower()] if customer_code is not None else []

    # Check the return code
    if result.returncode == 0:
        print("Capturing databricks jobs...")
        jobs: str = result.stdout
        print(f'Current jobs on databricks workflows: \n{jobs}')
        job_ids_by_client: Dict[str, List[str]] = capture_job_ids(jobs, ws_env)
        print(f"Captured jobs {job_ids_by_client}")

        client_job_info: Dict[str, Dict[str, Any]] = {}

        for client, ids in job_ids_by_client.items():
            client_code = str(client).lower()
            if len(filtered_client_jobs) == 0 or client_code in filtered_client_jobs:
                client_job_content: Dict[str, Any] = {}
                client_job_key: str = f'{ws_env}_deus_{client}_job_{current_branch}_branch'
                client_job_content['name'] = client_job_key
                client_job_content['queue'] = {'enabled': 'true'}
                client_job_params = {'ws_env': ws_env}

                # ... [resto del código para construir job_tasks sin cambios] ...

                client_job_content['tasks'] = [*job_tasks]
                client_job_content['email_notifications'] = { 'on_failure': '${workspace.current_user.userName}' }
                client_job_info[client_job_key] = client_job_content

                job_tasks: list[dict[str, Any]] = []

                for job in ids:
                    layer: str = job.split('_')[0]

                    if layer == 'bronze':
                        depends_on: list[dict[str, str]] = [{'task_key': 'landing'}]
                    else:
                        depends_on = None

                    job_id: int = int(job.split('_')[1])
                    task_obj = Task(task_key=layer, params=client_job_params, depends_on=depends_on)
                    task: dict[str, Any] = task_obj.create_job_runner_task(job_id= job_id)
                    job_tasks.append(task)

                client_job_content['tasks'] = [*job_tasks]
                client_job_content['email_notifications'] = { 'on_failure': '${workspace.current_user.userName}' }
                client_job_info[client_job_key] = client_job_content
                
        current_file_directory: str = os.path.dirname(os.path.abspath(__file__)).replace('/workflow_definition/job_helper', '')
        filepath: str = current_file_directory + '/resources/jobs.yml'   

        content: dict[str, Any] = load_yaml_file(file_path=filepath)
        
        for key, value in client_job_info.items():
            content['resources']['jobs'][key] = value

        save_yaml_file(filepath = filepath, content = content)

    else:
        print(f"Command failed with return code {result.returncode}")
        print("Error output:\n", result.stderr)


if __name__== '__main__':
    parser = argparse.ArgumentParser(description="Pack databricks inner job for workspace.")
    parser.add_argument("--target", required=True, help="Specify the target environment")
    parser.add_argument("--customer", required=False) # optional
    args = parser.parse_args()
    ws_env = args.target # workspace environment
    customer_code = args.customer
    main(ws_env=ws_env, customer_code=customer_code) 
