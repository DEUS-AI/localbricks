
import logging
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from deus_lib.abstract.abstract_task import Task
from deus_lib.utils.common import get_dbutils, write_table


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GenerateOBT(Task):
    def __init__(self):
        logger.info("Initializing CreateCTAS task")
        super().__init__()
        self.spark = SparkSession.builder.appName("Execute CTAS").getOrCreate()
        self.dbutils = get_dbutils()
        self.obt_path = "gold_layer.`client-vs`.obt"


    def load_data(self, **kwargs) -> DataFrame:
        self.generate_obt("Flight")

    
    def save_data(self, df: DataFrame, **kwargs) -> None:        
        write_table(df, self.obt_path, mode="overwrite")
        
        
            
    def get_satellites_for_hub(self, hub_name):
        sat_tables = [
            "SAT_IOT_Records",
            "SAT_Flight_Planned",
            "SAT_Fuel_Planned",
            "SAT_Fuel_Actual",
            "SAT_Flight_Actual",
            "SAT_Flight_IOT_Stats"
        ]
        satellites_dfs = []
        for sat in sat_tables:
            query = f"SELECT * FROM {sat} WHERE event_id IN (SELECT event_id FROM HUB_{hub_name})"
            sat_df = self.spark.sql(query)
            satellites_dfs.append(sat_df)

        combined_satellites_df = reduce(DataFrame.unionAll, satellites_dfs)
        
        return combined_satellites_df
    
    def get_hubs_and_satellites_for_link(self, link_name):
        hub_tables = ["HUB_Flight", "HUB_Time", "HUB_Crew_Member"]
        link_df = self.spark.sql(f"SELECT * FROM {link_name}_link")

        hubs_satellites_dfs = []
        for hub in hub_tables:
            hub_df = self.spark.sql(f"SELECT * FROM {hub} WHERE event_id IN (SELECT event_id FROM {link_name}_link)")
            hub_satellites_df = self.get_satellites_for_hub(hub.split("_")[1])
            hubs_satellites_dfs.append(hub_df)
            hubs_satellites_dfs.append(hub_satellites_df)

        combined_hubs_satellites_df = reduce(DataFrame.unionAll, hubs_satellites_dfs)
        return combined_hubs_satellites_df
    
    def generate_obt(self, entity_name, entity_type):
        """
        Generates the OBT (Orbit) DataFrame based on the given entity name and type.

        Args:
            entity_name (str): The name of the entity.
            entity_type (str): The type of the entity. Can be either 'hub' or 'link'.

        Returns:
            obt_df (DataFrame): The OBT DataFrame containing the joined data.

        Raises:
            ValueError: If the entity_type is not 'hub' or 'link'.
        """

        if entity_type == 'hub':
            hub_name = entity_name
            hub_df = self.spark.sql(f"SELECT * FROM HUB_{hub_name}")
            satellites_df = self.get_satellites_for_hub(hub_name)
            obt_df = hub_df.join(satellites_df, "event_id", "inner")
            
        elif entity_type == 'link':
            link_name = entity_name
            link_df = self.spark.sql(f"SELECT * FROM {link_name}_link")
            hubs_satellites_df = self.get_hubs_and_satellites_for_link(link_name)
            obt_df = link_df.join(hubs_satellites_df, "event_id", "inner")
            
        else:
            raise ValueError("Invalid entity_type. Must be either 'hub' or 'link'.")

        return obt_df
    
if __name__ == "__main__":
    task = GenerateOBT()
    task.main()