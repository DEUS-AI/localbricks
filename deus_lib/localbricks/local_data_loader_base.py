import os
from pyspark.sql import SparkSession

class DataLoaderBase:
    """
    A base class for data loaders that initializes a Spark session and sets the current working directory.

    Attributes:
        spark (SparkSession): The Spark session.
        cwd (str): The current working directory.
    """

    def __init__(self):
        """
        Initializes the DataLoaderBase with a Spark session and sets the current working directory.
        """

        self.spark = self.init_spark_session()
        self.cwd = os.path.dirname(os.path.abspath(__file__))

    @staticmethod
    def init_spark_session() -> SparkSession:
        """
        Initializes and returns a Spark session with Delta Lake and Delta Sharing configurations.

        Returns:
            SparkSession: The initialized Spark session.
        """

        spark = SparkSession.builder.appName("DeltaSharingExample") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:3.1.0") \
            .getOrCreate()
        return spark