from typing import Dict
from pydantic import BaseModel, Field

from .task import Task


class Job(BaseModel):
    """
    Represents a job in the workflow.

    Attributes:
        name (str): The name of the job.
        tasks (Dict[str, Task]): A dictionary of tasks associated with the job.
    """
    
    name: str
    tasks: Dict[str, Task]

