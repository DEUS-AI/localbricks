from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Any, Optional

class Task(ABC):

    def pre_task(self, **kwargs) -> None:
        """
        Abstract method to perform something before ingesting data.
        """
        pass
    
    @abstractmethod
    def load_data(self, **kwargs) -> DataFrame:
        """
        Abstract method to load data.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def process_data(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Abstract method to process data.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def save_data(self, df: DataFrame, **kwargs) -> None:
        """
        Abstract method to write the DataFrame to the given target.
        Must be implemented by subclasses.
        """
        pass
    

    def post_task(self, **kwargs) -> None:
        """
        Abstract method to perform something after ingesting data.
        """
        pass    
    

    def main(self, pre_task_args: Optional[dict[str, Any]] = {}, load_data_args: Optional[dict[str, Any]] = {}, process_data_args: Optional[dict[str, Any]] = {}, save_data_args: Optional[dict[str, Any]] = {}):
        """
        Common main method that reads, transforms, and writes data.
        """
        self.pre_task(kwargs= pre_task_args)
        df = self.load_data(kwargs = load_data_args)
        processed_df = self.process_data(df = df, kwargs = process_data_args)
        self.save_data(df = processed_df, kwargs = save_data_args)
        self.post_task()


