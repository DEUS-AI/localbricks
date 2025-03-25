import re
import sys, os

from deus_lib.utils.common import initialize_job, parse_task_params
pattern = r'^(.*?/files)'
match = re.search(pattern, os.path.abspath(__name__))
sys.path.append(match.group(1))

import json
import logging
from pyspark.sql import SparkSession
from deus_lib.abstract.abstract_task import Task
from deus_lib.silver_factory.silver_base_task import SilverBaseTask
from src.tasks.clients.common_merge import import_client_module, load_mapping_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CreateCTAS(SilverBaseTask):
    
    def __init__(self):
        logger.info("Initializing CreateCTAS task")
        super().__init__()
        self.settings = load_mapping_file(self.consumer_code)
        self.merge_table = self.settings["dataMappings"]["catalog"] + self.settings["dataMappings"]["merged_table"] 
        
    
    def load_data(self):
        df = self.spark.table(self.merge_table)                
        return df
    
    
    def process_data(self, data):
        ctas = import_client_module(client_code=self.cc, module_name='ctas')
        behaviours = import_client_module(self.cc, module_name="behaviours")

        data = behaviours.run(data, self.settings["dataMappings"]["calculableBehavious"])
                    
        reference_schema = ctas.remove_unexisting_columns(data)

        for table_name, schema in reference_schema.items():
            self.spark.sql(f"DROP TABLE IF EXISTS silver_layer.`client-{self.cc}`.{table_name}")
            ctas.generate_ctas(data, f"silver_layer.`client-{self.cc}`.{table_name}", schema)        
            
    def save_data(self, data):        
        pass
    
    def post_task(self, **kwargs) -> None:
        return super().post_task(**kwargs)  

    
if __name__ == "__main__":
    spark, dbutils = initialize_job()
    client_file_settings, task_params = parse_task_params()
    process_config = {
        'client_settings': client_file_settings,
        'task_params': task_params,
        'spark': spark,
        'dbutils': dbutils
    }    
    task = CreateCTAS(config = process_config)
    task.main() 