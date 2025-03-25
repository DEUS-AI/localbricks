from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

from .file_parser import FileParser
from .file_pattern import FilePattern


class Task(BaseModel):
    """
    Represents a task in the workflow.

    Attributes:
        name (str): The name of the task.
        description (Optional[str]): The description of the task (default: None).
        file_pattern (Optional[FilePattern]): The file pattern for the task (default: None).
        compression (Optional[str]): The compression method for the task (default: None).
        python_file (Optional[str]): The Python file associated with the task (default: None).
        parameters (Dict[str, Any]): The parameters for the task (default: {}).
        file_parser (Optional[FileParser]): The file parser for the task (default: None).
        output_table_name (Optional[str]): The name of the output table (default: None).
    """
    name: str
    output_table_name: Optional[str] = None
    description: Optional[str]
    format: Optional[str] = None
    compression: Optional[Union[bool, str]] = False
    python_file: Optional[str] = None    
    file_pattern: Optional[FilePattern] = None
    file_parser: Optional[FileParser] = None
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    depends_on: Optional[List[str]] = Field(default_factory=list)

    def __str__(self):
        return (f"Task(name={self.name}, description={self.description}, file_pattern={self.file_pattern}, "
                f"compression={self.compression}, python_file={self.python_file}, parameters={self.parameters}, "
                f"file_parser={self.file_parser}, output_table_name={self.output_table_name}, depends_on={self.depends_on})")