from pydantic import BaseModel, field_validator, Field, model_validator
from typing import ClassVar, Optional
from datetime import datetime
import json, os

def extract_client_names(file_path_from_root: str) -> set[str]:
    """Extracts client names from a JSON file.

    Args:
        file_path_from_root (str): The path to the JSON file, relative to the root of the project.

    Returns:
        set[str]: A set of client names extracted from the JSON file.

    Raises:
        FileNotFoundError: If the specified file is not found.
        ValueError: If the file contains invalid JSON data.
    """

    current_file_directory: str = os.path.dirname(os.path.abspath(__file__)).replace('/deus_lib/utils', '')
    full_path: str = os.path.join(current_file_directory, file_path_from_root)
    client_names = []

    
    try:
        with open(full_path, 'r') as file:
            data = json.load(file)

        for client_settings in data:
            client_name = client_settings['client_name'].split('-')[1]
            client_names.append(client_name)

        return set(client_names)
    except FileNotFoundError:
        raise FileNotFoundError("The customer codes file was not found.")
    except json.JSONDecodeError:
        raise ValueError("Error decoding JSON from the customer codes file.")
    
class TaskJobParamConfig(BaseModel):
    job_run_id: str
    dds_code: Optional[str] = None
    skip_ingestion: Optional[bool] = None
    ingest_start_datetime: Optional[str] = None
    ingest_end_datetime: Optional[str] = None
    load_type: Optional[str] = None
    domain: Optional[str] = None
    ws_env: Optional[str]

    valid_dds_codes: ClassVar[set[str]] = {'TUI', 'VS', 'ST', 'CGL', 'PGA', 'LDC', 'CHR'}
    valid_domains: ClassVar[set[str]] = {'aviation', 'maritime'}

    @field_validator('domain')
    @classmethod
    def check_domain(cls, v: str) -> str:
        if v is None:
            return v
        else:
            if v not in cls.valid_domains:
                raise ValueError(f'Invalid domain name: {v}, available domains: {cls.valid_domains}')
            else:
                return v

    @field_validator('dds_code')
    @classmethod
    def check_dds_code(cls, v: str) -> str:
        if v is None:
            return v
        else:
            dds_code = v.upper()
            if dds_code not in cls.valid_dds_codes:
                raise ValueError(f'Invalid DDS code: {v}, available DDS codes: {cls.valid_dds_codes}')
            else:
                return dds_code

    @field_validator('load_type')
    @classmethod
    def check_load_type(cls, v: str) -> str:
        if v is None:
            return v
        else:
            load_type = v.lower()

            if load_type not in ('full', 'incremental'):
                raise ValueError(f'Invalid load_type {load_type}, you can only choose full or incremental')
            else:
                return v

    @field_validator('ingest_start_datetime', 'ingest_end_datetime')
    @classmethod
    def check_datetime_format(cls, v: str) -> str:
        if v is None:
            return v
        elif v == 'now':
            return datetime.now().strftime('%Y-%m-%d %H:%M')
        else:
            try:
                datetime_obj = datetime.strptime(v, '%Y-%m-%d %H:%M')
                return datetime_obj.strftime('%Y-%m-%d %H:%M')
            except ValueError:
                raise ValueError(f"Datetime must be 'now' or in YYYY-MM-DD HH:MM format, got {v}")

    @model_validator(mode='after')
    def check_dates(self) -> 'TaskJobParamConfig':
        if self.ingest_start_datetime is not None and self.ingest_end_datetime is not None:
            start_datetime = datetime.strptime(self.ingest_start_datetime, '%Y-%m-%d %H:%M')
            end_datetime = datetime.strptime(self.ingest_end_datetime, '%Y-%m-%d %H:%M')

            if end_datetime < start_datetime:
                raise ValueError("End datetime must not be earlier than start datetime")

        return self
    
if __name__ == '__main__':

    try:
        config = TaskJobParamConfig(
            job_run_id='661966544802159',
            dds_code='TUI',
            ingest_start_datetime='2010-01-01 17:20',
            ingest_end_datetime='now',
            load_type='incremental',
            domain = 'aviation'
            )
        print(config)
    except Exception as e:
        print(str(e))