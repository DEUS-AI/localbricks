from typing import List, Dict, Optional
from pydantic import BaseModel, model_validator

from .task import Task


class JobLayer(BaseModel):
    """
    Represents a job layer.

    Attributes:
        csv (Dict[str, Task]): A dictionary that maps CSV keys to Task objects.
    """
    tasks: Dict[str, Task]
    job_parameters: Optional[Dict[str, str]] = None
