import yaml, git
from typing import Any
import os

def display_building_msg(task_key: str):
    """
    Prints a message indicating the start of a building task.

    Args:
        task_key (str): The key of the task that is being started.

    Returns:
        None
    """

    print(f'Starting building task {task_key}')

def load_yaml(file_path: str) -> dict:
    """
    Load a YAML file and return its contents as a dictionary.
    
    Args:
        file_path (str): Path to the YAML file
        
    Returns:
        dict: Contents of the YAML file
    """
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def load_yaml_file(file_path: str) -> dict:
    """
    Load a YAML file and return its contents as a dictionary.
    
    Args:
        file_path (str): Path to the YAML file
        
    Returns:
        dict: Contents of the YAML file
    """
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def save_yaml_file(filepath: str, content: dict) -> None:
    """
    Save content to a YAML file.
    
    Args:
        filepath (str): Path where to save the YAML file
        content (dict): Content to save
    """
    with open(filepath, 'w') as f:
        yaml.dump(content, f, default_flow_style=False)

def get_current_branch() -> str:
    """
    Retrieves the name of the current active branch in the git repository.

    Returns:
        str: The name of the current active branch.
    """
    repo = git.Repo(search_parent_directories=True)
    branch = repo.active_branch.name
    return branch.replace('/', '_')
