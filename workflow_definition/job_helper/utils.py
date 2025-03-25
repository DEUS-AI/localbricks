import yaml, git
from typing import Any

def display_building_msg(task_key: str):
    """
    Prints a message indicating the start of a building task.

    Args:
        task_key (str): The key of the task that is being started.

    Returns:
        None
    """

    print(f'Starting building task {task_key}')

def load_yaml_file(file_path: str) -> dict[str, Any]:
    """
    Loads and parses a YAML file.

    Args:
        file_path (str): The path to the YAML file to be loaded.

    Returns:
        Dict[str, Any]: The content of the YAML file as a dictionary.

    Raises:
        RuntimeError: If there is an error in loading the YAML file.
    """

    try:
        with open(file_path) as file:
            content: dict[str, Any]  = yaml.safe_load(file)
        return content
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Error in loading {file_path}: {exc}")


def save_yaml_file(filepath: str, content: dict) -> None:
    """
    Saves content to a YAML file.

    Args:
        filepath (str): The path to the YAML file to be saved.
        content (Dict[str, Any]): The content to be saved to the YAML file.

    Returns:
        None
    """
    with open(filepath, 'w') as file:
        yaml.safe_dump(content, file, sort_keys=False, default_flow_style=False)


def get_current_branch() -> str:
    """
    Retrieves the name of the current active branch in the git repository.

    Returns:
        str: The name of the current active branch.
    """
    repo = git.Repo(search_parent_directories=True)
    branch = repo.active_branch.name
    return branch.replace('/', '_')
