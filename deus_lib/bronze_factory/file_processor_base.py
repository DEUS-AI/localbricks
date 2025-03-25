from deus_lib.utils.common import sanitize_column_names, order_columns_df, write_table, get_current_london_timestamp, remove_whitespace_from_column_names
from deus_lib.abstract.abstract_task import Task
from deus_lib.utils.decorators import log_method_data
from deus_lib.bronze_factory.bronze_pydantic_validation import IngestionConfig
import logging, datetime, re
from itertools import islice
from pyspark.sql import DataFrame, Row
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from typing import Any, Iterable, Optional
from urllib.parse import unquote

# BronzeBaseTask
class IngestionFilesBase(Task):
    """Base class to ingest files as source."""

    def __init__(self, config: dict[str, Any]):
        """
         Initializes the IngestionFilesBase class.

        Args:
            config(dict[str, Any]): A dictionary containing configuration parameters for the ingestion process.
        """
        super().__init__()

        logging.info(f"Validating config: {config}")
        validated_config = self.__validate_config(config)

        self.spark: SparkSession = validated_config.spark
        self.client_code: str = f"client-{validated_config.task_params.customer_code.lower()}"
        self.industry: str = validated_config.task_params.industry
        
        self.ws_env: str = validated_config.task_params.ws_env
        self.job_run_id: str = validated_config.task_params.job_run_id
        self.ingest_start_date: str = validated_config.task_params.ingest_start_datetime
        self.ingest_end_date: str = validated_config.task_params.ingest_end_datetime
        self.file_type: str = validated_config.file_type
        
        ws_landing_catalog_schema = f"{self.industry}__landing_zone__{self.ws_env}.`{self.client_code}`"
        ws_bronze_catalog_schema = f"{self.industry}__bronze_layer__{self.ws_env}.`{self.client_code}`"
            
        self.file_processing_table: str = f"{ws_landing_catalog_schema}.file_processing_table"
        
        self.output_table_name: str = f"{ws_bronze_catalog_schema}.`{validated_config.client_settings.output_table_name}`".lower()
        self.user_options: dict[str, Any] = validated_config.client_settings.file_parser.args.get_non_none_dict()
        
        self.naming_pattern: str = self.__retrieve_name_pattern(validated_config= validated_config)
        self.base_file_settings: dict = self.base_init_file_settings()
    
    @staticmethod
    def __retrieve_name_pattern(validated_config: IngestionConfig) -> Optional[str]:
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

    @staticmethod
    def __validate_config(config):
        try:
            validated_config = IngestionConfig(**config)
            return validated_config
        except Exception as e:
            logging.error(f"Configuration validation failed: {e}")
            raise Exception(f"Configuration validation failed: {e}")

    @log_method_data
    def base_init_file_settings(self) -> dict[str,str]:
        """
        Initializes file settings for reading data using spark.

        This method combines default file settings with user-provided options.

        Returns:
            dict(dict[str,str]): A dictionary containing the final file settings.
        """

        base_file_settings: dict = {
                "inferSchema": "false",
                "columnNameOfCorruptRecord": "corrupted_data",
                "mergeSchema": "true"
            }

        base_file_settings.update(self.user_options)

        logging.info(f"File settings: {base_file_settings}")

        return base_file_settings
    
    @log_method_data
    def extract_paths(self) -> list[str]:
        """
        Extracts file paths based on file type, status, and dates.

        This method retrieves file paths from the `file_processing_table` for a specific client based on the
        specified file type, status, and ingestion start and end dates.

        Returns:
            lists[str]: A list of file paths.
        """

        logging.debug(f'Extracting filepath for file format {self.file_type}')

        if self.naming_pattern:
            name_filter=f"AND filename RLIKE '{self.naming_pattern}'"
        else:
            name_filter = ""

        query = f"""
                    SELECT filepath 
                    FROM {self.file_processing_table}
                    WHERE format = '{self.file_type}'
                        AND (status != 'ingested' AND status != 'glacier_file')
                        AND last_modified > '{self.ingest_start_date}'
                        AND last_modified < '{self.ingest_end_date}'
                        {name_filter}
        """

        logging.info(f"Query used: {query}")

        file_paths = [row.filepath for row in self.spark.sql(query).collect()]

        return file_paths
    
    @log_method_data
    def load_data(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        Reads data from the specified file paths.

        This method uses the `file_type` and `base_file_settings` to read data from the provided file paths.

        Args:
            **kwargs (dict[str, Any]): A dictionary of keyword arguments.
                - source (list[str]): A list of file paths to read data from.

        Returns:
            DataFrame: A Spark DataFrame containing the read data.

        Example:
            >>> config = {'source': ['path/to/file1.csv', 'path/to/file2.csv']}
            >>> df = load_data(**config)
        """

        source: list[str] = kwargs.get('source', [])
        df: DataFrame = self.spark.read.format(self.file_type).options(**self.base_file_settings).load(source)


        return df
    
    def process_data(self, df: DataFrame, **kwargs: dict[str, Any]) -> DataFrame:
        """
        Processes data for the specified df.

        Args:
            df (DataFrame): A dataframe with the data to be processed
            **kwargs (dict[str, Any]): A dictionary of keyword arguments.
                - decorating_args (dict[str, Any]): kwargs for decorating the df.
                - cleaning_args dict[str, Any]): kwargs for cleaning the df.

        Returns:
            DataFrame: A Spark DataFrame containing the read data.

        Example:
            >>> config = {'source': ['path/to/file1.csv', 'path/to/file2.csv']}
            >>> df = load_data(**config)
        """

        decorating_args: dict[str, Any] = kwargs.get('decorating_args', [])
        cleaning_args: dict[str, Any] = kwargs.get('cleaning_args', [])
        decorated_df = self.decorating(df=df, **decorating_args)
        cleansed_df = self.cleaning(df=decorated_df, **cleaning_args)

        return cleansed_df
        
    
    @log_method_data
    def decorating(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Adds metadata columns info to the DataFrame.

        This method adds columns for source file name, status, ingestion date, job run ID, and file path.

        Args:
            df(DataFrame): The input DataFrame.
            **kwargs (dict[str, Any]): A dictionary of keyword arguments.

        Returns:
            DataFrame: The DataFrame with added metadata columns.
        """

        current_london_timestamp: datetime.datetime = get_current_london_timestamp()
        iso_datetime_format: str = current_london_timestamp.isoformat()

        decode_path_udf = F.udf(lambda path: unquote(path), StringType())

        decorated_df: DataFrame = (
                        df.select(
                            "*",
                            F.expr("_metadata.file_name").alias('source_file_name'),
                            F.lit('incomplete').alias('status'),
                            F.lit(iso_datetime_format).cast("timestamp").alias('ingestion_datetime'),
                            F.lit(self.job_run_id).alias('ingestion_job_run_id'),
                            decode_path_udf(F.expr("_metadata.file_path")).alias('file_path')
                        )
                    )

        modified_df: DataFrame = decorated_df.withColumn(
                "file_path",
                F.regexp_replace(F.col("file_path"), "^dbfs:", "")
            )

        return modified_df
    
    @log_method_data
    def cleaning(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Cleans the DataFrame and reorders columns.

        This method sanitizes column names, reorders columns based on clustering keys, and handles special characters.

        Args:
            df(DataFrame): The input DataFrame.
            **kwargs (dict[str, Any]): A dictionary of keyword arguments.
                - cluster_keys (list[str]): A list of cluster keys (columns) to be used on liquid clustering. In this case to order
                    the dataframe, having the columns used as cluster_keys in the beginning, so it is possible to collect statistics to 
                    be used by liquid clustering.

        Returns:
            DataFrame: The cleaned and reordered DataFrame.
        """

        cluster_keys: list[str] = kwargs.get('cluster_keys', [])
        # characters not supported in delta tables: https://docs.delta.io/latest/delta-batch.html#use-special-characters-in-column-names
        special_characters: dict = {
            '_': ['.', ';', ',', '(', ')', '{', '}']
        }

        cleansed_df: DataFrame = sanitize_column_names(df = df, special_characters = special_characters)
        ordered_df: DataFrame = order_columns_df(df = cleansed_df, columns_first = cluster_keys)

        whitespace_removal = remove_whitespace_from_column_names(ordered_df)

        return whitespace_removal
    
    @log_method_data
    def save_data(self, df: DataFrame, **kwargs) -> None:
        """
        Writes the DataFrame to a Delta table.

        This method writes the DataFrame to a Delta table, performs optimization, and updates the ingestion status.

        Args:
            df(DataFrame): The DataFrame to be written.
            **kwargs (dict[str, Any]): A dictionary of keyword arguments.
                - cluster_keys (list[str]): A list of cluster keys to be used by liquid clustering when writing the df.
        """

        cluster_keys: list = kwargs.get('cluster_keys', [])
        output_table_name: str = kwargs.get('output_table_name')
        write_table(sdf= df, full_table_name= output_table_name, cluster_keys= cluster_keys, change_data_feed = 'false')
        #self.spark.sql(f'OPTIMIZE {output_table_name}')

    @log_method_data
    def log_ingestion_status(self, source: str | list[Row], status: str = 'ingested') -> None:
        """
        Updates the ingestion status of file(s) in the `file_processing_table`.

        This method can handle both single and multiple file updates.

        Args:
            source (str | list[tuple[str, str]]): Either a single file path (str) or a list of (filepath, status) tuples.
            status (str, optional): The new status of the file. Required if source is a single file path.
        """

        status = status[:255]

        if isinstance(source, str):
            # Single file update
            logging.info(f'Updating filepath {source} with status {status}')
            query = f"""
            UPDATE {self.file_processing_table}
            SET status = "{status}"
            WHERE filepath = "{source}"
            """
        elif isinstance(source, list):
            # Multiple file update
            if not source:
                return
            
            filepaths = [(row['file_path'],status) for row in source]

            for filepath, file_status in filepaths:
                logging.info(f'Updating filepath {filepath} with status {file_status}')

            # Create a temporary view with the new statuses
            status_df = self.spark.createDataFrame(filepaths, ["filepath", "new_status"])
            status_df.createOrReplaceTempView("new_statuses")

            query = f"""
            MERGE INTO {self.file_processing_table} AS t
            USING new_statuses AS n
            ON t.filepath = n.filepath
            WHEN MATCHED THEN
                UPDATE SET t.status = n.new_status
            """
            
        else:
            raise ValueError("Invalid input type for source")

        self.spark.sql(query)

    @staticmethod
    def batched(iterable: Iterable[str], n: int) -> list[list[str]]:
        """
        Splits an iterable into batches of a specified size.

        Args:
            iterable(Iterable[str]): The iterable to be split.
            n(int): The size of each batch.

        Yields:
            list[list[str]]: Batches of elements from the iterable.
        """
        it = iter(iterable)
        while True:
            batch = list(islice(it, n))
            if not batch:
                break
            yield batch

    def split_corrupted_data(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Splits a DataFrame into corrupted and non-corrupted dataframes based on 'corrupted_data' column.

        This method identifies corrupted data (based on the presence of 'corrupted_data' column) and
        splits the DataFrame into two: one containing corrupted records and another containing non-corrupted records.

        Args:
            df(DataFrame): The input DataFrame.

        Returns:
            tuple[DataFrame, DataFrame]: A tuple containing the corrupted DataFrame and the non-corrupted DataFrame.
        """
        
        if 'corrupted_data' not in df.columns:
            return None, df
        
        df.cache()
        
        corrupted_file_paths = df.filter(F.col('corrupted_data').isNotNull()) \
                                .select('file_path').distinct()
        corrupted_paths = [row['file_path'] for row in corrupted_file_paths.collect()]
        
        corrupted_df = df.filter(F.col('file_path').isin(corrupted_paths))
        non_corrupted_df = df.filter(~F.col('file_path').isin(corrupted_paths))

        df.unpersist()
        
        return corrupted_df, non_corrupted_df
    
    
    def save_and_log(self, df, write_args: dict[str, Any], table_suffix='', status='ingested') -> None:
            
        if df and not df.isEmpty():
            catalog, schema, table = [part.replace('`', '') for part in self.output_table_name.lower().split('.')]
            output_table = f"`{catalog}`.`{schema}`.`{table}{table_suffix}`"
            write_args['output_table_name'] = output_table
            self.save_data(df=df, **write_args)

            paths = df.select('file_path').distinct().collect()
            self.log_ingestion_status(source=paths, status=status)

    def pipeline_steps(self, list_paths: list[str], decorating_args: dict[str,Any], cleaning_args: dict[str,Any], write_args: dict[str, Any]) -> None:
        """
        Performs the ingestion pipeline steps for a batch of files.

        This method reads data from the specified file paths, performs decoration, cleaning, and writing
        to the Delta table, handling corrupted records separately.

        Args:
            list_paths(list[str]): A list of file paths.
            decorating_args(dict[str,Any]): Arguments to be passed to the `decorating` method.
            cleaning_args(dict[str,Any]): Arguments to be passed to the `cleaning` method.
            write_args(dict[str,Any]): Arguments to be passed to the `write` method.
        """

        load_task_args = {'source': list_paths}
        df = self.load_data(**load_task_args)

        if df.columns == ['corrupted_data']:
            raise Exception("File completely corrupted")
        elif df.isEmpty():
            logging.info("Dataframe is empty, stoping pipeline now, no need to continue as there is no data to be added")
            return

        
        processing_args = {'decorating_args': decorating_args, 'cleaning_args': cleaning_args}
        cleansed_df = self.process_data(df = df, **processing_args)

        corrupted_df, non_corrupted_df = self.split_corrupted_data(cleansed_df)
        
        self.save_and_log(non_corrupted_df, write_args= write_args)
        self.save_and_log(corrupted_df, write_args= write_args, table_suffix= '_corrupted', status= 'corrupted_records: consult table with corrupt records for more info')


    def run_event_file_driven_pipeline(self, decorating_args: dict[str,Any] = {}, cleaning_args: dict[str,Any] = {}, write_args: dict[str,Any] = {}) -> None:
        """
        Runs the event-driven ingestion pipeline for files.

        This method extracts file paths, splits them into microbatches, and processes each batch using the
        `pipeline_steps` method, handling errors and individual file processing if necessary.

        Args:
            decorating_args(dict[str,Any]): Arguments to be passed to the `decorating` method.
            cleaning_args(dict[str,Any]): Arguments to be passed to the `cleaning` method.
            write_args(dict[str,Any]): Arguments to be passed to the `write` method.
        """
        
        file_paths = self.extract_paths()
        batch_size = 50
        batches = list(self.batched(file_paths, batch_size))

        for list_paths in batches:
            
            if not isinstance(list_paths, list):
                list_paths = [list_paths]

            logging.info(f"Loading files from paths {list_paths}")

            try:
                
                self.pipeline_steps(list_paths= list_paths, decorating_args= decorating_args, cleaning_args= cleaning_args, write_args= write_args)

            except Exception as e:
                logging.error(f"Error loading files from paths {list_paths}: {e}")
                logging.info('Loading files individually to detect the file(s) with the issue')
                for file_path in list_paths:

                    try:
                        self.pipeline_steps(list_paths= [file_path], decorating_args= decorating_args, cleaning_args= cleaning_args, write_args= write_args)

                    except Exception as e:

                        logging.error(f"Error loading file from path {file_path}: {e}")
                        self.log_ingestion_status(source=file_path, status=f'Error loading file: {e}')