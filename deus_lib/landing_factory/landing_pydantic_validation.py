from pydantic import BaseModel, Field
from typing import Any, Optional

class BaseTaskParams(BaseModel):
    job_run_id: str
    customer_code: str
    industry: str
    ingest_start_datetime: Optional[str]
    ingest_end_datetime: Optional[str]
    load_type: Optional[str] = 'incremental'
    ws_env: Optional[str]
    
class FileLoaderTaskParams(BaseTaskParams):
    load_type: str

class UncompressFilePattern(BaseModel):
    name: Optional[str] = None
    min_size_bytes: Optional[int] = 0

class FileLoaderFilePattern(BaseModel):
    name: str
    min_size_bytes: Optional[int] = 0

class BaseClientSettings(BaseModel):
    file_pattern: Optional[UncompressFilePattern] = None

class StrictClientSettings(BaseModel):
    file_pattern: FileLoaderFilePattern

class BaseFilesLoaderConfig(BaseModel):
    spark: Any = Field(..., description="SparkSession object")
    dbutils: Any = Field(..., description="DBUtils object")
    task_params: BaseTaskParams
    client_settings: BaseClientSettings
    file_type: str

class StrictFilesLoaderConfig(BaseFilesLoaderConfig):
    task_params: FileLoaderTaskParams
    client_settings: StrictClientSettings
