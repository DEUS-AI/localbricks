from pydantic import BaseModel, Field
from typing import Any, Optional

class TaskParams(BaseModel):    
    job_run_id: str
    dds_code: str
    domain: str
    ingest_start_datetime: Optional[str]
    ingest_end_datetime: Optional[str]
    ws_env: Optional[str]

class Args(BaseModel):
    header: str
    header_line: Optional[int] = 0
    skipRows: Optional[int] = None
    multiline: Optional[str] = None
    rowTag: Optional[str] = None
    sep: Optional[str] = ','

    def get_non_none_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.dict().items() if v is not None}

class FileParser(BaseModel):
    args: Args

class FilePattern(BaseModel):
    name: Optional[str] = None

class DataSettings(BaseModel):
    output_table_name: str
    file_parser: FileParser
    file_pattern: Optional[FilePattern] = None

class IngestionConfig(BaseModel):
    spark: Any = Field(..., description="SparkSession object")
    file_type: str
    task_params: TaskParams
    data_settings: DataSettings