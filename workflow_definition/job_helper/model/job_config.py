from typing import Dict, List, Optional
from pydantic import BaseModel, model_validator

LAYERS = ['landing', 'bronze', 'silver', 'gold']
from .job_layer import JobLayer


class JobConfig(BaseModel):
    """
    Represents the configuration for a job.

    Attributes:
        customer_code (str): The customer code.
        industry (str): The industry.
        landing (JobLayer): The landing job layer.
        bronze (Optional[JobLayer]): The bronze job layer (optional).
        silver (Optional[Dict[str, Any]]): The silver job layer (optional).
        gold (Optional[Dict[str, Any]]): The gold job layer (optional).
    """
    customer_code: str
    industry: str
    landing: Dict[str, JobLayer]
    bronze: Optional[Dict[str, JobLayer]] = None
    silver: Optional[Dict[str, JobLayer]] = None
    gold: Optional[Dict[str, JobLayer]] = None
