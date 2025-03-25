import re
import sys, os
pattern = r'^(.*?/files)'
match = re.search(pattern, os.path.abspath(__name__))
sys.path.append(match.group(1))

import logging
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from deus_lib.silver_factory.silver_base_task import SilverBaseTask
from deus_lib.utils.common import get_dbutils, write_table
from src.tasks.clients.common_merge import *
from src.tasks.clients.common_behaviours import *

TYPE_MAP = {
    "string": "string",
    "date":   "timestamp",
    "number": "decimal(18,5)",
    "boolean": "boolean"
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MergeDataframes(SilverBaseTask):

    def __init__(self, config):
        logger.info("Initializing MergeDataframes task")        
        super().__init__(config=config)                
        self.settings = load_mapping_file(self.consumer_code)
        
        
        self.dfs = {}

        
    def load_data(self, **kwargs) -> DataFrame:
        logger.info("Loading data")
        for schema in self.settings["dataMappings"]["data_frames"]:
            table_name = self.settings["dataMappings"]["catalog"] + schema
            
            df = self.spark.table(table_name)
            
            self.dfs[schema] = df
            
        return df

    def process_data(self, df: DataFrame, **kwargs) -> DataFrame:
        logger.info("Processing data")
        
        if self.settings["dataMappings"]["industry"] == "aviation":
            df_merged = self._process_aviation(self.dfs)
            
        elif self.settings["dataMappings"]["industry"] == "maritime":
            df_merged = self._process_maritime(self.dfs)
            
        else:
            logger.error("missing industry")        
                
        return df_merged        
        
    def save_data(self, df: DataFrame, **kwargs) -> None:
        logger.info("Saving data")
        df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy(self.settings["dataMappings"]["join_keys"]).saveAsTable(f"{self.settings['dataMappings']['catalog']}merged_data")
        
    def post_task(self, **kwargs) -> None:
        df = self.spark.table(self.merge_table)
                
        df = apply_formulas(df, self.consumer_code)
        
        df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy(self.settings["dataMappings"]["join_keys"]).saveAsTable(f"{self.settings['dataMappings']['catalog']}merged_data")        
    
    def _process_maritime(self, dfs):
        client_merger = import_client_module(client_code=self.consumer_code, module_name='merge')       
        
        if client_merger == None:
            df_merged = mariapps_merge(dfs)
            df_merged = clean_df_header(df_merged)
            df_merged = apply_mapping_for_customer(self.consumer_code, df_merged)                        
            
            return df_merged
        else:            
            df_merged = m.merge(list(dfs.values()))
            df_merged = clean_df_header(df_merged)
            df_merged = apply_mapping_for_customer(self.consumer_code, df_merged)
        
            return df_merged

    def _process_aviation(self, dfs):
        logger.info("Processing data")
        
        client_merger = import_client_module(client_code=self.consumer_code, module_name='merge')
                
        for schema, temp_df in self.dfs.items():
            
            logger.info(f"Processing data for schema {schema}")
            temp_df = clean_df_header(temp_df)
            temp_df = apply_mapping_for_customer(self.consumer_code, temp_df) 
            temp_df = temp_df.orderBy('ocurrence_datetime')
            self.dfs[schema] = temp_df

        data_frames_list = list(self.dfs.values())
        
        df_merged = client_merger.merge(data_frames_list)

        logger.info(f"Data merged with columns {df_merged.columns}")
            
        df_merged = remove_spaces(df_merged) # no need for this in my new approach
        df_merged = add_default_columns_spark(df_merged)
        df_merged = ensure_all_fields(df_merged)        
        
        df_merged = df_merged.withColumn("event_openid", F.col("event_deus_id"))
        
        df_merged = apply_formulas(df_merged, self.consumer_code)       
        
        return df_merged 
        
if __name__ == "__main__":
    task = MergeDataframes()
    task.main()
    