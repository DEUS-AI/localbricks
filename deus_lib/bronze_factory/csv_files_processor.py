import logging
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
from functools import reduce
from deus_lib.utils.decorators import log_method_data
from deus_lib.utils.common import disambiguate_column_names
from deus_lib.bronze_factory.file_processor_base import IngestionFilesBase
from typing import Any
from collections import defaultdict, OrderedDict

class IngestCsvFiles(IngestionFilesBase):
    """Class for ingesting CSV files with a specific header format."""

    def __init__(self, config: dict[str, Any]):
        """
        Initializes the IngestCsvFiles class.

        Args:
            config(dict): A dictionary containing configuration parameters for the ingestion process.
        """
        super().__init__(config=config)
        self.csv_file_settings, self.header_line = self.csv_init_file_settings()

    def csv_init_file_settings(self) -> tuple[dict[str, str], int]:
        """
        Initializes file settings for reading csv data using spark.

        This method combines default file settings with user-provided options and handles the header.

        Returns:
            dict(dict[str,str]): A dictionary containing the final file settings.
            int: The header line number.
        """
        base_file_settings = self.base_file_settings

        # Extract the 'header' option from user options, with a default value of 0
        header_line = base_file_settings.pop('header_line', 0)

        logging.info(f"File settings: {base_file_settings} and header_line {header_line}")

        return base_file_settings, header_line
    
    def extract_header(self, source: str, header_line: int, seps: list) -> tuple[list[str],str]:
        """
        Extracts the header from a CSV file.

        This method reads the third line of a CSV file and returns it as a list of column names.
        It also modifies the first column name to 'time' and adds 'corrupted_data' to the end.

        Args:
            source(str): The path to the CSV file.
            header_line(int): The line number of the header, starting from 0

        Returns:
            tuple: A tuple containing:
            - list: A list of column names from the extracted header.
            - str: The separator that was successfully used.
        """

        with open(source, 'r') as file:
            header = None
            for i, line in enumerate(file):
                if i == header_line:  # Extract the third line as the header
                    for current_sep in seps:
                        header = line.strip().split(current_sep)
                        if len(header) > 1:
                            if self.client_code == 'client-tui':
                                header[0] = 'time'
                                header.append('corrupted_data')
                            return header, current_sep
                        
        # If we've tried all separators and none worked, raise an exception
        raise ValueError(f"Unable to extract a valid header from the file {source} with any of the specified separators.")
    
    @staticmethod
    def build_schema(header: list[str]) -> StructType:
        """
        Builds a Spark schema from a header.

        This method creates a Spark schema based on the provided header, assigning StringType to each column.

        Args:
            header(list): A list of column names.

        Returns:
            StructType: A Spark schema representing the structure of the data.
        """

        schema = StructType([StructField(col, StringType(), True) for col in header])

        return schema
                
    def create_final_headers_and_schemas(self, micro_batches: dict[str, tuple[list, list]]) -> dict[str, tuple[list, StructType]]:
        """
        Create final headers for each separator in the micro-batches, maintaining order and removing duplicates.
        Keeps the original file list and creates a single schema for each separator.

        Args:
            micro_batches (dict): A dictionary with separators as keys and tuples of (file_paths, headers) as values.

        Returns:
            dict: A dictionary with separators as keys and tuples of (file_paths, schema) as values,
                  where schema is a Spark StructType based on the final header.
        """
        final_micro_batches = {}
        for sep, (file_paths, headers) in micro_batches.items():
            final_header = OrderedDict()
            for header in headers:
                for column in header:
                    final_header[column] = None  # Using None as a placeholder
            final_header_list = list(final_header.keys())
            schema = self.build_schema(final_header_list)
            final_micro_batches[sep] = (file_paths, schema)
        return final_micro_batches


    @staticmethod
    def create_sub_micro_batch(source_header_sep: dict[str, tuple[list, str]], seps: list[str]) -> dict[str, tuple[list, list]]:
        """
        Create sub-micro batches based on separators.

        Args:
            source_header_sep (dict): A dictionary with file paths as keys and tuples of (header, separator) as values.
            seps (list): List of separators to consider.

        Returns:
            dict: A dictionary with separators as keys and tuples of (file_paths, headers) as values.
        """
        micro_batches = defaultdict(lambda: ([], []))

        for file_path, (header, current_sep) in source_header_sep.items():
            if current_sep in seps:
                file_paths, headers = micro_batches[current_sep]
                file_paths.append(file_path)
                headers.append(header)

        return dict(micro_batches)
    

    @staticmethod
    def union_dataframes_with_missing_columns(dfs):
        # Get all unique column names across all DataFrames
        all_columns = set()
        for df in dfs:
            all_columns.update(df.columns)
        
        # Add missing columns to each DataFrame
        completed_dfs = []
        for df in dfs:
            missing_columns = all_columns - set(df.columns)
            new_df = df
            for column in missing_columns:
                new_df = new_df.withColumn(column, lit(None))
            completed_dfs.append(new_df.select(sorted(all_columns)))
        
        # Union all DataFrames
        return reduce(DataFrame.unionByName, completed_dfs)

    
    @log_method_data
    def load_data(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        Reads CSV files with a custom header extraction and schema creation.

        This method reads multiple CSV files, extracts headers from each file, builds a combined schema,
        and loads the data into a Spark DataFrame.

        Args:
            **kwargs (dict[str, Any]): A dictionary of keyword arguments.
                - source(list[str]): A list of paths to CSV files.

        Returns:
            DataFrame: A Spark DataFrame containing the read data.
        """

        source: list[str] = kwargs.get('source', [])

        logging.info(f'source files {source}')

        sep = self.csv_file_settings['sep']
        seps = sep.split('/') if '/' in sep else [sep]
        source_header_sep = {}

        for file_path in source:  

            header, sep = self.extract_header(source = file_path, header_line = self.header_line, seps = seps)
            header = disambiguate_column_names(columns= header)
            source_header_sep[file_path] = [header,sep]

        micro_batches = self.create_sub_micro_batch(source_header_sep=source_header_sep, seps = seps)
        final_micro_batches = self.create_final_headers_and_schemas(micro_batches= micro_batches)

        logging.info(f'Loaded schema {final_micro_batches}')

        logging.info(f"Creating dataframe with specs: {self.file_type} and options {self.csv_file_settings}")

        dfs = []
        for sep, (source, schema) in final_micro_batches.items():
            self.csv_file_settings['sep'] = sep
            df: DataFrame = self.spark.read.format(self.file_type).schema(schema).options(**self.csv_file_settings).load(source)
            dfs.append(df)

        return self.union_dataframes_with_missing_columns(dfs)