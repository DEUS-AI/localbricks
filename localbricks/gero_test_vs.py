from localbricks_factory import data_loader_manager
import pyspark.sql.functions as F


def test_localbricks(pandas_df):
    print(pandas_df.head())

if __name__ == '__main__':
    delta_sharing_table_alias = "landing_vs.file_processing_table" # original table name is "landing_zone.client-vs.file_processing_table"
    delta_df = data_loader_manager(action='load_directly', share='geronimo_localbricks_2', schema_table_name=delta_sharing_table_alias, limit=100)
    # using pyspark
    status_counts = delta_df.groupBy("status").count()
    status_counts.show()
    print('------------')
    # using pandas (in memory)
    pandas_df = delta_df.toPandas()
    print(pandas_df.head())
    status_counts = pandas_df['status'].value_counts()
    print(status_counts)
