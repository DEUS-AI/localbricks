import logging
from localbricks.local_data_loader_base import DataLoaderBase
from deus_lib.utils.common import load_json
from typing import Optional
from pyspark.sql import DataFrame

class DeltaSharingLoader(DataLoaderBase):
    """
    A class to load data from Delta Sharing and manage local storage updates.

    Attributes:
        profile_path (Optional[str] = None): The path to the Delta Sharing profile.
        table_config_path (str): The path to the table configuration JSON file.
        local_table_config (list[dict[str,str]]): The loaded table configuration.
    """

    def __init__(self, profile_path: Optional[str] = None, table_config_path: Optional[str] = None):
        """
        Initializes the DeltaSharingLoader with the given profile path and table configuration path.

        Args:
            profile_path (Optional[str]): The path to the Delta Sharing profile. Defaults to None.
            table_config_path (Optional[str]): The path to the table configuration JSON file. Defaults to None.
        """

        super().__init__()
        self.profile_path = profile_path if profile_path != None else f"{self.cwd}/config.share"
        table_config_path = table_config_path if table_config_path != None else f"{self.cwd}/local_tables_config.json"
        self.local_table_config: list[dict[str,str]] = load_json(table_config_path)

    def directly_load_dataframe(self, share: str, schema_table_name: str, limit:int = 10) -> DataFrame:
        """
        Directly loads a DataFrame using Delta Sharing.

        Args:
            share (str): The share name.
            schema_table_name (str): the name of the table with the schema, so schema.table_name: bronze_layer.orders

        Returns:
            DataFrame: The loaded DataFrame.
        """

        table_url = self.profile_path + f"#{share}.{schema_table_name}"

        options = {
                "responseFormat": "delta"
            }

        logging.info(f'Reading delta table: {table_url}')
        
        # https://docs.databricks.com/en/data-sharing/read-data-open.html#access-shared-data-using-spark
        df = self.spark.read.format("deltaSharing").options(**options).load(table_url).limit(limit)

        return df
    
    def update_storage(self, schema_table_name: str, share: str) -> None:
        """
        Updates the local storage with data from Delta Sharing.

        Args:
            schema_table_name (str): The schema table name.
            share (str): The share name.
        """

        df = self.directly_load_dataframe(share = share, schema_table_name = schema_table_name)
        df_coalesced = df.coalesce(1)
        storage_path = f'{self.cwd}/storage/{schema_table_name}'
        df_coalesced.write.mode("overwrite").parquet(storage_path)
        df = self.spark.read.parquet(storage_path)
        logging.info(f'Updated/added {schema_table_name} successfully')


    def control_update_storage(self, schema_tables: Optional[list[str]] = None, update_all: bool = False) -> None:
        """
        Controls the update of local storage for specified schema tables or all tables.

        Args:
            schema_tables (Optional[List[str]]): The list of schema tables to update. Defaults to None.
            update_all (bool): Flag to indicate if all tables should be updated. Defaults to False.
        """

        try:

            if update_all:
                
                for table_config in self.local_table_config:

                    share = table_config['share']
                    schema_table_name = table_config['schema.table_name']
                    logging.info(f'Updating table {schema_table_name}')
                    self.update_storage(schema_table_name = schema_table_name, share = share)
            
            elif schema_tables != None:

                for schema_table_name in schema_tables:

                    for table_config in self.local_table_config:

                        if schema_table_name == table_config['schema.table_name']:

                            logging.info(f'Updating table or adding {schema_table_name}')
                            share = table_config['share']
                            self.update_storage(schema_table_name = schema_table_name, share = table_config['share'])

                        else:
                            raise ValueError(f'Table not found in path {self.table_config_path}, please add it')
            
        except Exception as e:
            logging.error(f"Error writing or reading parquet for table {schema_table_name}: {e}")
            raise
