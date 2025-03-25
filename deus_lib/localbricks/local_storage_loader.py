import logging
from typing import Optional
from pyspark.sql import DataFrame

from localbricks.local_data_loader_base import DataLoaderBase

class LocalStorageLoader(DataLoaderBase):
    """
    A class to load data from local storage.

    Attributes:
        table_path (Optional[str]): The path to the table in local storage.
    """

    def __init__(self, schema_table_name: str, table_path: Optional[str] = None):
        """
        Initializes the LocalStorageLoader with the given schema table name and optional table path.

        Args:
            schema_table_name (str): the name of the table with the schema, so schema.table_name: bronze_layer.orders
            table_path (Optional[str]): The path to the table in local storage. Defaults to None.
        """

        super().__init__()
        self.table_path = f'{self.cwd}/storage/{schema_table_name}' if table_path is None else table_path

    def load_dataframe_from_local_storage(self) -> DataFrame:
        """
        Loads a DataFrame from local storage.

        Returns:
            DataFrame: The loaded DataFrame from the local storage.
        """

        logging.info(f'Loading from local storage table stored in {self.table_path}')
        df = self.spark.read.parquet(self.table_path)

        return df