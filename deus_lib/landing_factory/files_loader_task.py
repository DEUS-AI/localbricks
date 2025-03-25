import datetime, logging, re
from typing import Any, Optional
from pydantic import ValidationError

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql import Row

from deus_lib.utils.decorators import log_method_data
from deus_lib.abstract.abstract_task import Task
from deus_lib.utils.common import checksum, write_table
from deus_lib.landing_factory.landing_pydantic_validation import StrictFilesLoaderConfig, BaseFilesLoaderConfig

# LandingBaseTask
class FilesLoaderTask(Task):
    
    def __init__(self, config: dict[str, Any]):

        super().__init__()
        validated_config = self._validate_config(config)

        self.spark: SparkSession = validated_config.spark
        self.dbutils: DBUtils = validated_config.dbutils
        self.client_code: str = f"client-{validated_config.task_params.customer_code.lower()}"
        self.industry: str = validated_config.task_params.industry
        self.ws_env: str = validated_config.task_params.ws_env
        self.job_run_id: str = validated_config.task_params.job_run_id
        self.ws_landing_catalog = f"{self.industry}__landing_zone__{self.ws_env}"
        self.volume: str = f"/Volumes/{self.ws_landing_catalog}/{self.client_code}/raw_files"
        self.file_processing_table: str = f"{self.ws_landing_catalog}.`{self.client_code}`.file_processing_table"
        self.base_dir = self.volume
        self.file_type: str = validated_config.file_type
        self.naming_pattern: str = self.__retrieve_name_pattern(validated_config= validated_config)
        self.min_size_bytes: int = self.__retrieve_min_size_bytes(validated_config= validated_config)
        self.ingest_start_datetime: str = validated_config.task_params.ingest_start_datetime
        self.ingest_end_datetime: str = validated_config.task_params.ingest_end_datetime
        self.load_type = validated_config.task_params.load_type

    @staticmethod
    def _validate_config(config: dict[str, Any]) -> StrictFilesLoaderConfig:
        try:
            return StrictFilesLoaderConfig(**config)
        except ValidationError as e:
            logging.error(f"Configuration validation failed: {e}")
            raise ValueError(f"Configuration validation failed: {e}")
        
    @staticmethod
    def __retrieve_min_size_bytes(validated_config: StrictFilesLoaderConfig | BaseFilesLoaderConfig) -> Optional[int]:
        
        if validated_config.client_settings.file_pattern:
            return validated_config.client_settings.file_pattern.min_size_bytes
        else:
            return None
        

    @staticmethod
    def __retrieve_name_pattern(validated_config: StrictFilesLoaderConfig | BaseFilesLoaderConfig) -> Optional[str]:
        """
        Retrieves and processes the file naming pattern from the validated configuration.

        This method extracts the file pattern name from the client settings, processes it
        by replacing multiple backslashes with a single backslash, and returns the result.

        Args:
            validated_config (IngestionConfig): The validated configuration object containing
                                                client settings and file pattern information.

        Returns:
            Optional[str]: The processed naming pattern as a string, or None if no pattern
                           is found or if processing results in None.

        Example:
            If the raw pattern is "data\\\\file_\\d{8}\\.csv", this method will return
            "data\\file_\\d{8}\\.csv".
        """

        if validated_config.client_settings.file_pattern:
            raw_pattern = validated_config.client_settings.file_pattern.name
            naming_pattern = re.sub(r'\\+', r'\\', raw_pattern) if raw_pattern is not None else None
            return naming_pattern
        else:
            return None
        
    def pre_task(self, **kwargs):
        logging.info(f'Starting landing task for customer {self.client_code} and bucket {self.volume}')

        
    @log_method_data
    def __loading_max_datetime_file_processing_table(self) -> datetime.date:

        table_name = self.file_processing_table.split('.')[-1]
        logging.info(f"`{self.industry}landing_zone`.`{self.client_code}`")
        table_exists: bool = self.spark.sql(f'SHOW TABLES IN {self.industry}__landing_zone__{self.ws_env}.`{self.client_code}`').filter(F.col('tableName') == table_name).count() > 0

        if table_exists:
            max_exists: bool = self.spark.sql(f"SELECT count(*) AS total_records FROM {self.file_processing_table} WHERE format = '{self.file_type}' ").first()['total_records']>0
        else:
            max_exists = False
        
        logging.info(f"Table exists: {table_exists}")

        if max_exists:
            max_datetime = self.spark.sql(f"SELECT MAX(last_modified) AS max_datetime FROM {self.file_processing_table} WHERE format = '{self.file_type}' ").first()['max_datetime'].replace(second = 0).strftime("%Y-%m-%d %H:%M")
        else:
            max_datetime = self.ingest_start_datetime
            
        return max_datetime 
    
    @log_method_data
    def __time_boundary(self) -> tuple[datetime.datetime]:

        default_ingest = False

        if self.ingest_start_datetime == '2010-01-01 17:21':
            if self.load_type == 'incremental':
                logging.info(f"Going with default incremental load, using max datetime present on file processing table")
                max_datetime = self.__loading_max_datetime_file_processing_table()
                default_ingest = True
            elif self.load_type == 'full':
                logging.info(f"Going with default full load")
                max_datetime = self.ingest_start_datetime
            else:
                raise ValueError(f"Load type not supported {self.load_type}")
            return max_datetime, self.ingest_end_datetime, default_ingest
        else:
            logging.info(f"Custom {self.load_type} load")
            return self.ingest_start_datetime, self.ingest_end_datetime, default_ingest
    
    
    def __filter_all_files_dataframe(self, all_files: list[Any], min_datetime: str, max_datetime: str, default_ingest: bool) -> DataFrame:

        df = self.spark.createDataFrame(all_files)
        df.createOrReplaceTempView("files")

        logging.info(f"Running query on source files with min datetime as {min_datetime} and max datetime as {max_datetime}")

        query_source_files = f"""
                                WITH table AS (
                                    SELECT 
                                        regexp_replace(path, '^dbfs:', '') AS path, 
                                        name, 
                                        size, 
                                        TO_TIMESTAMP(FROM_UNIXTIME(modificationTime / 1000)) AS modificationTime
                                    FROM 
                                        files
                                    WHERE 
                                        name RLIKE '{self.naming_pattern}'
                                        AND size >= {self.min_size_bytes} 
                                )
                                SELECT 
                                    * 
                                FROM 
                                    table 
                                WHERE 
                                    modificationTime > TIMESTAMP '{min_datetime}'
                                    AND modificationTime <= TIMESTAMP '{max_datetime}'
                                """
        
        df_source_paths = self.spark.sql(query_source_files)

        if self.load_type == 'incremental' and not default_ingest:

            logging.info("Custom incremental load with anti-join pattern")
            query_target_files = f"""
                                    SELECT 
                                        filepath, 
                                        last_modified 
                                    FROM 
                                        {self.file_processing_table}
                                    WHERE 
                                        format = '{self.file_type}'
                                        AND last_modified > TIMESTAMP '{min_datetime}'
                                        AND last_modified < TIMESTAMP '{max_datetime}'
                                        AND (status = 'ingested' or status = 'pending')
                                """

            df_target_paths = self.spark.sql(query_target_files)

            # Perform the left anti join
            df_new_files = df_source_paths.alias('source').join(
                df_target_paths.alias('target'),
                (F.col('source.path') == F.col('target.filepath')) & 
                (F.col('source.modificationTime') == F.col('target.last_modified')),
                'leftanti'
            )

            # If you want to select only specific columns from the result
            final_df = df_new_files.select('source.path', 'source.name', 'source.size', 'source.modificationTime')
        
        else: 
            final_df = df_source_paths

        total_records_source = df_source_paths.count()
        total_records_to_write = final_df.count()
        min_datetime_present = final_df.agg(F.min("modificationTime").alias("min_datetime")).first()['min_datetime']
        max_datetime_present = final_df.agg(F.max("modificationTime").alias("max_datetime")).first()['max_datetime']

        logging.debug(f"Running with total records on source {total_records_source}, total records to write {total_records_to_write}, min_datetime_present {min_datetime_present} and max_datetime_present {max_datetime_present}")

        return final_df

    
    def __list_all_files(self, min_datetime: str, max_datetime: str, default_ingest: bool) -> list[Row]:

        all_files = []
        dirs_to_process = [self.base_dir]  # Initialize with the root directory

        while dirs_to_process:
            current_dir = dirs_to_process.pop(0)
            listed_content = self.dbutils.fs.ls(current_dir)

            for content in listed_content:
                if content.isDir():
                    dirs_to_process.append(content.path)  # Add directory to the queue
                else:
                    #content_date = datetime.datetime.fromtimestamp(content.modificationTime / 1000)
                    #pattern = re.compile(self.naming_pattern)
                    #if pattern.match(content.name) is not None and content.size > self.min_size_bytes and content_date >= min_datetime and content_date <= max_datetime:
                    all_files.append(content)  # Add file to the list

            logging.info(f'Processing directory: {current_dir}')

        df = self.__filter_all_files_dataframe(all_files = all_files, min_datetime = min_datetime, max_datetime = max_datetime, default_ingest = default_ingest)
 
        all_files_rows = df.collect()

        return all_files_rows
    

    def _is_glacier_file(self, file_path: str) -> bool:
        try:
            self.dbutils.fs.head(file_path, 1)
            return False
        except Exception as e:
            message = str(e)
            if "InvalidObjectState" in message:
                return True
            else:
                raise Exception(f'Other exception besides the storage class was found {e}')

    
    # Function to list non-Glacier files in the S3 bucket using dbutils.fs
    def _list_non_glacier_files(self, all_files: list[Row]) -> list[tuple[str]]:
        rows = []
        
        try:
            for file in all_files:

                try:

                    # Check if file is not in Glacier and was modified in the last two months
                    file_mod_time = file.modificationTime
                    file_name = file.name
                    file_format = file_name.split('.')[-1]
                    file_size = file.size
                    file_path = file.path

                    if not self._is_glacier_file(file.path):
                        logging.info('Starting checksum')
                        file_etag = checksum(file.path.replace('dbfs:',''))
                        logging.info(f"Adding row with config {(file_path, file_format, 'pending', file_etag, file_mod_time, file_size)}")
                        rows.append((file_path, file_name, file_format, 'pending', file_etag, file_mod_time, file_size))
                    else:
                        file_etag = None
                        logging.info(f"Adding row with config {(file_path, file_format, 'glacier_file', file_etag, file_mod_time, file_size)}")
                        rows.append((file_path, file_name, file_format, 'glacier_file', file_etag, file_mod_time, file_size))

                except Exception as e:
                    logging.error(f'Error loading file {file.path}, error {e}') 
                    rows.append((
                    file_path if 'file_path' in locals() else None, 
                    file_name if 'file_name' in locals() else None,
                    file_format if 'file_format' in locals() else None, 
                    str(e), 
                    file_etag if 'file_etag' in locals() else None,
                    file_mod_time if 'file_mod_time' in locals() else None, 
                    file_size if 'file_size' in locals() else None
                ))

        except Exception as e:
            raise Exception(f"Error listing files in {self.volume}: {e}")
        
        return rows
    
    def __listing_files(self):

        logging.info('Loading time boundary')

        min_datetime, max_datetime, default_ingest = self.__time_boundary()

        logging.info("Loading data from S3.")
        all_files = self.__list_all_files(min_datetime = min_datetime, max_datetime = max_datetime, default_ingest = default_ingest)

        if not all_files:

            logging.info("No files found to process.")
            return []
        else:

            files = self._list_non_glacier_files(all_files = all_files) 
            return files
    
    def _create_dataframe(self, files: list[tuple[str]]) -> DataFrame:

        schema = StructType([
            StructField("filepath", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("format", StringType(), True),
            StructField("status", StringType(), True),
            StructField("etag", StringType(), True),
            StructField("last_modified", TimestampType(), True),
            StructField("size", LongType(), True)
        ])

        files_df = self.spark.createDataFrame(files, schema)
        return files_df
    
    def load_data(self, **kwargs) -> DataFrame:

        files = self.__listing_files()

        if not files:
            logging.info("No files found to process.")
        else:
            logging.info(f"Found {len(files)} total of files to process.")

        return self._create_dataframe(files=files)

    def process_data(self, df: DataFrame, **kwargs) -> DataFrame:
        logging.info("Only ingestion, no processing")
        return df

    def save_data(self, df: DataFrame, **kwargs) -> None:
        logging.info("Saving data to processing table.")
        write_table(sdf = df, full_table_name = self.file_processing_table, cluster_keys = ['format','status','filename','last_modified'], change_data_feed = 'false')

    def post_task(self, **kwargs) -> None:
        logging.info("File paths loaded.")
    

# Example usage:
if __name__ == "__main__":
    pass
