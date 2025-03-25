from typing import Optional
from pydantic import BaseModel

class FilePattern(BaseModel):
    """
    Represents a file pattern used for matching files based on certain criteria.

    Attributes:
        name (str): The name of the file pattern.
        name_regex (str): The regular expression used for matching file names.
        min_size_bytes (int): The minimum size of the files in bytes.
        format (str): The format of the file pattern.
    """
    name: str
    name_regex: str
    min_size_bytes: Optional[int] = 0
    format: Optional[str] = None
    