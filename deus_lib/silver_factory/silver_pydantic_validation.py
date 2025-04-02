from pydantic import BaseModel, Field
from typing import Any, Optional

class JobParams(BaseModel):
    job_run_id: str
    dds_code: str
    
class SilverTaskConfig(BaseModel):
    spark: Any = Field(..., description="SparkSession object")
    dbutils: Any = Field(..., description="DBUtils object")
    task_params: JobParams