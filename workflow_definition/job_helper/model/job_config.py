from typing import Dict, List, Optional
from pydantic import BaseModel, model_validator

LAYERS = ['landing', 'bronze', 'silver', 'gold']
from .job_layer import JobLayer


class JobConfig(BaseModel):
    """
    Represents the configuration for a job.

    Attributes:
        dds_code (str): The Domain Data Store code.
        domain (str): The domain type (e.g., aviation, maritime, etc.).
        landing (JobLayer): The landing job layer.
        bronze (Optional[JobLayer]): The bronze job layer (optional).
        silver (Optional[Dict[str, Any]]): The silver job layer (optional).
        gold (Optional[Dict[str, Any]]): The gold job layer (optional).
    """
    dds_code: str
    domain: str
    landing: Dict[str, JobLayer]
    bronze: Optional[Dict[str, JobLayer]] = None
    silver: Optional[Dict[str, JobLayer]] = None
    gold: Optional[Dict[str, JobLayer]] = None
