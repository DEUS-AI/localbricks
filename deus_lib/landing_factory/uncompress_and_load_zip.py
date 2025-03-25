import logging
from typing import Any
from pydantic import ValidationError

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import Row
import pyspark.sql.functions as F

from deus_lib.utils.common import extract_zip_file
from deus_lib.landing_factory.files_loader_task import FilesLoaderTask
from deus_lib.utils.common import write_table
from deus_lib.landing_factory.landing_pydantic_validation import BaseFilesLoaderConfig

class UncompressIngestZipTask(FilesLoaderTask):
    """_summary_

    Args:
        Task (_type_): _description_
    """
    
    def __init__(self, config: dict[str, Any]):

        super().__init__(config=config)
        self.base_extraction_folder: str = f"{self.volume}/extracted_files"

    @staticmethod
    def _validate_config(config: dict[str, Any]) -> BaseFilesLoaderConfig:
        try:
            return BaseFilesLoaderConfig(**config)
        except ValidationError as e:
            logging.error(f"Configuration validation failed: {e}")
            raise ValueError(f"Configuration validation failed: {e}")

    def pre_task(self, **kwargs): 

        try:
            self.dbutils.fs.mkdirs(self.base_extraction_folder)
            logging.info(f"Created extracted files folder in case it does not exit")
            
        except Exception as e:
            logging.error(f"Error creating folder for extracted files: {e}")


    def __transform_all_files(self, all_files: list[Any]) -> list[Row]:

        # Create DataFrame from the list of FileInfo objects
        df = self.spark.createDataFrame(all_files)

        df_transformed = df.select(
            F.expr("regexp_replace(path, '^dbfs:', '')").alias("path"),
            "name",
            "size",
            F.to_timestamp(F.from_unixtime(F.col("modificationTime") / 1000)).alias("modificationTime")
        )

        # Collect the results and return as a list of Row objects
        return df_transformed.collect()

    def load_data(self, **kwargs):

        df = self.spark.table(self.file_processing_table).filter((col("format") == "zip") & (col("status") == "pending")).collect()

        all_rows = []
        
        for row in df:
            zip_file_path = row.filepath
            zip_file_name = row.filename
            extraction_folder = f'{self.base_extraction_folder}/{zip_file_name}'
            self.dbutils.fs.mkdirs(extraction_folder)

            if extract_zip_file(zip_file_path = zip_file_path, extraction_folder = extraction_folder):

                logging.info(f"Successfully extracted {zip_file_path} to {self.base_extraction_folder}")
                
                self.base_dir = extraction_folder

                all_files = self.dbutils.fs.ls(self.base_dir)

                all_files = self.__transform_all_files(all_files)

                file_row = self._list_non_glacier_files(all_files = all_files)
                all_rows.append(*file_row)
                all_rows.append((zip_file_path, zip_file_name, 'zip', 'ingested', row.etag, row.last_modified, row.size))

            else:

                logging.error(f"Failed to unzip {zip_file_path}")
                all_rows.append((zip_file_path, zip_file_name, 'zip', 'failed to unzip', row.etag, row.last_modified, row.size))

        df = self._create_dataframe(files = all_rows)
        return df
    
    def save_data(self, df: DataFrame, **kwargs) -> None:
        logging.info("Saving data to processing table.")
        write_table(sdf = df, full_table_name = self.file_processing_table, cluster_keys = ['format','status','filename','last_modified'], change_data_feed = 'false', upsert = True, merge_columns = ['filepath','last_modified'])

if __name__ == "__main__":
    pass
