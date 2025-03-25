from pyspark.sql import SparkSession

class SparkSessionSingleton:
    """
    A singleton class for creating and accessing a SparkSession.

    This class ensures that only one SparkSession instance is created and reused throughout the application.
    """

    _instance = None

    def __init__(self):
        """
        Initializes the SparkSessionSingleton instance.

        Raises:
            Exception: If an instance of SparkSessionSingleton already exists.
        """
        if SparkSessionSingleton._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            self.spark = self._create_spark_session()
            SparkSessionSingleton._instance = self

    @staticmethod
    def _create_spark_session() -> SparkSession:
        """
        Creates and initializes a SparkSession with the necessary configurations.

        Returns:
            SparkSession: The created SparkSession instance.
        """
    # Initialize a SparkSession with KryoSerializer and registered classes

        spark = SparkSession.builder \
            .appName("Deus") \
            .getOrCreate()
    
        return spark

    def get_spark_session(self) -> SparkSession:
        """
        Retrieves the current active SparkSession.

        Returns:
            SparkSession: The active SparkSession instance.
        """
        return self.spark.getActiveSession()

    @classmethod
    def get_instance(cls) -> 'SparkSessionSingleton':
        """
        Retrieves the singleton instance of SparkSessionSingleton.

        If no instance exists, a new one is created.

        Returns:
            SparkSessionSingleton: The singleton instance of the class.
        """
        if cls._instance is None:
            cls()
        return cls._instance
