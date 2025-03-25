import datetime
import logging
from typing import Any
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame
from deus_lib.abstract.abstract_task import Task
from deus_lib.silver_factory.silver_pydantic_validation import SilverTaskConfig

class SilverBaseTask(Task):
    
    def __init__(self, config: dict[str, Any]):
        super().__init__()
        validated_config = self.__validate_config(config)
        
        self.industry: str = validated_config.task_params.industry        
        self.ws_env: str = validated_config.task_params.ws_env        
        self.spark: SparkSession = validated_config.spark
        self.dbutils: DBUtils = validated_config.dbutils
        self.consumer_code = validated_config.task_params.customer_code.lower()
        self.client_code: str = f"client-{validated_config.task_params.customer_code.lower()}"
        self.ws_bronze_catalog_schema = f"{self.industry}__bronze_layer__{self.ws_env}.`{self.client_code}`"
        self.merge_table: str = f"{self.ws_bronze_catalog_schema}.merged_data"

    @staticmethod
    def __validate_config(config):
        try:
            validated_config = SilverTaskConfig(**config)
            return validated_config
        except Exception as e:
            logging.error(f"Configuration validation failed: {e}")
            raise Exception(f"Configuration validation failed: {e}")
        
    def pre_task(self, **kwargs):
        logging.info(f'Starting silver task for customer {self.client_code}')
    
    def load_data(self, **kwargs) -> DataFrame:
        pass

    def process_data(self, df: DataFrame, **kwargs) -> DataFrame:
        logging.info("Processing data in Silver layer.")
        return df

    def save_data(self, df: DataFrame, **kwargs) -> None:
        logging.info("Saving data to processing table.")
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.merge_table)

    def post_task(self, **kwargs) -> None:
        logging.info("File paths processed and loaded.")

# Example usage:
if __name__ == "__main__":
    pass