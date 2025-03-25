import logging
from pyspark.sql import DataFrame
from deus_lib.utils.common import convert_structs_to_json
from deus_lib.bronze_factory.file_processor_base import IngestionFilesBase
from typing import Any

class IngestXmlFiles(IngestionFilesBase):
    """Class for ingesting XML files."""

    def __init__(self, config: dict[str, Any]):
        """
        Initializes the IngestXmlFiles class.

        Args:
            config(dict[str, Any]): A dictionary containing configuration parameters for the ingestion process.
        """

        super().__init__(config = config)

    def cleaning(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Cleans the input DataFrame, converting structs to JSON strings.

        This method overrides the `cleaning` method from the base class. 
        It first calls the `cleaning` method of the base class to perform any initial cleaning steps.
        Then, it converts any structs in the DataFrame to JSON strings using the `convert_structs_to_json` function.

        Args:
            df(DataFrame): The input DataFrame to be cleaned.
            **kwargs: Additional keyword arguments to be passed to the cleaning method of the base class.

        Returns:
            DataFrame: The cleaned DataFrame with structs converted to JSON strings.
        """
        
        cleansed_df = super().cleaning(df = df, **kwargs)

        logging.info("Converting from struct into json string")
        cleansed_df = convert_structs_to_json(df = cleansed_df)

        return cleansed_df