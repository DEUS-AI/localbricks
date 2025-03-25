import hashlib
import zipfile
import json, sys, logging, datetime
import boto3
from urllib.parse import urlparse
from typing import Any
from enum import Enum
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from deus_lib.utils.sparksession import SparkSessionSingleton
from deus_lib.utils.pydantic_validation import TaskJobParamConfig


def convert_datetime_string_to_datetime(datetime_str: str, datetime_format: str) -> datetime.datetime:
        return datetime.datetime.strptime(datetime_str, datetime_format)


def remove_whitespace_from_column_names(df: DataFrame) -> DataFrame:
    """
    Removes leading and trailing whitespace from all column names in a DataFrame.

    Args:
        df (DataFrame): The input DataFrame with column names to be cleaned.

    Returns:
        DataFrame: A new DataFrame with the same data but with column names stripped of leading and trailing whitespace.
    """
    
    current_columns = df.columns
    new_columns = [F.col(c).alias(c.strip()) for c in current_columns]
    return df.select(new_columns)

def convert_structs_to_json(df: DataFrame) -> DataFrame:
    """
    Converts all StructType fields in a DataFrame to JSON strings.

    Args:
        df (DataFrame): The input DataFrame with potentially StructType fields.

    Returns:
        DataFrame: The DataFrame with StructType fields converted to JSON strings.
    """

    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            df = df.withColumn(field.name, F.to_json(F.col(field.name)))
    return df


def disambiguate_column_names(columns: list[str]) -> list[str]:
    """
    Disambiguates column names by appending a numerical suffix to duplicate names.

    This function takes a list of column names and ensures that each name is unique.
    If a column name appears more than once, subsequent occurrences are appended
    with a numerical suffix (1, 2, etc.) to make them unique.

    Args:
        columns (List[str]): List of column names to be disambiguated.

    Returns:
        List[str]: List of unique column names with numerical suffixes for duplicates.

    Example:
        >>> columns = ["column1", "column2", "column1", "column3", "column2"]
        >>> disambiguate_column_names(columns)
        ['column1', 'column2', 'column1_1', 'column3', 'column2_1']
    """
    counter = {}
    result = []
    for column in columns:
        if column in counter:
            counter[column] += 1
            new_name = f"{column}_{counter[column]}"
        else:
            counter[column] = 0
            new_name = column
        result.append(new_name)
    return result

def get_dbutils() -> Any:
    """
    Retrieves the dbutils object for interacting with Databricks utilities.

    This function attempts to import the `dbutils` object from the IPython user namespace.
    It is typically used within a Databricks notebook.

    Returns:
        Any: The dbutils object.

    Raises:
        ImportError: If `dbutils` is not found, indicating the code is not running in a Databricks environment.
    """
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils
    except Exception as e:
        raise ImportError("Could not find dbutils. Ensure this code is running in a Databricks notebook or an environment where dbutils is available.") from e


def clean_temp_location(directory_path: str, dbutils: Any) -> None:
    """
    Deletes a temporary directory in a Databricks environment.

    This function removes the specified directory and all its contents
    using the provided dbutils object.

    Args:
        directory_path (str): The path to the directory to be deleted.
        dbutils (Any): The dbutils object used for file system operations.

    Returns:
        None

    Example:
        >>> dbutils = get_dbutils()
        >>> clean_temp_location("/tmp/some_directory", dbutils)
    """

    dbutils.fs.rm(directory_path, recurse=True)


def create_directory(directory_path: str, dbutils: Any) -> None:
    """
    Creates a directory in a Databricks environment.

    This function creates the specified directory using the provided dbutils object.

    Args:
        directory_path (str): The path to the directory to be created.
        dbutils (Any): The dbutils object used for file system operations.

    Returns:
        None

    Example:
        >>> dbutils = get_dbutils()
        >>> create_directory("/tmp/new_directory", dbutils)
    """    
    dbutils.fs.mkdirs(directory_path)


def sanitize_column_names(df: DataFrame, special_characters: dict[str,list]) -> DataFrame:
    """
    Sanitizes column names in a DataFrame by replacing special characters.

    This function replaces special characters in column names with specified replacements.
    It uses backticks to safely handle column names containing special characters.

    Args:
        df(DataFrame): The DataFrame whose column names need to be sanitized.
        special_characters(dict[str,list]): A dictionary mapping replacement characters to lists of characters to replace.
            For example:
            ```python
            special_characters = {
                "_": ["-", " ", "."],
                " ": ["_"]
            }
            ```
            This would replace hyphens, spaces, and periods with underscores, and underscores with spaces.

    Returns:
        The DataFrame with sanitized column names.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame([("col-1", 1), ("col 2", 2)], ["col-1", "col 2"])
        >>> special_characters = {"_": ["-", " "]}
        >>> sanitized_df = sanitize_column_names(df, special_characters)
        >>> sanitized_df.show()
        +------+---+
        |col_1 |  1|
        |col_2 |  2|
        +------+---+
    """

    def get_sanitized_col_name(col_name: str, char_map: dict[str,list]) -> str:
        """
        Sanitizes a single column name by replacing special characters.

        Args:
            col_name(str): The original column name.
            char_map(dict[str,list]): The dictionary mapping replacement characters to lists of characters to replace.

        Returns:
            The sanitized column name.
        """

        for replacement, chars in char_map.items():
            for char in chars:
                if char in col_name:
                    col_name = col_name.replace(char, replacement)
        return col_name

    # Generate SQL SELECT expressions for each column with sanitized names
    select_exprs = []
    for col_name in df.columns:
        sanitized_name: str = get_sanitized_col_name(col_name, special_characters)
        # Use backticks to safely handle original column names that contain special characters
        select_exprs.append(F.expr(f"`{col_name}` AS `{sanitized_name}`"))

    # Use selectExpr with all generated expressions to apply all renamings in a single transformation
    return df.select(*select_exprs)

def write_table(sdf: DataFrame, full_table_name: str, cluster_keys: list[str], change_data_feed: str = 'true', upsert: bool = False, merge_columns: list[str] = None) -> None:
    """
    Writes a DataFrame to a Delta table.

    This function writes the provided DataFrame to a Delta table in the specified database and schema.
    If the table doesn't exist, it creates a new Delta table with clustering on the specified keys.
    If the table exists, it appends data to the existing table, merging the schemas if necessary.

    Args:
        sdf(DataFrame): The DataFrame to be written to the table.
        full_table_name(str): The fully qualified name of the table, including database and schema, in the format `database.schema.table_name`.
        cluster_keys(list[str]): A list of column names to use for clustering the Delta table.

    Raises:
        Exception: If an error occurs during table creation or writing.
    """
    
    catalog, schema, table = [part.replace('`', '') for part in full_table_name.lower().split('.')]

    try:

        #check if table exists already
        table_exists: bool = sdf.sparkSession.sql(f'SHOW TABLES IN `{catalog}`.`{schema}`').filter(F.col('tableName') == table).count() > 0
        logging.info(f"Table exists: {table_exists}")
        
        #if table does not exist yet
        if not table_exists:
            sdf.write.format("delta").clusterBy(cluster_keys).option("delta.columnMapping.mode", "name").option("delta.checkpointPolicy", "classic").option("delta.enableChangeDataFeed", change_data_feed).saveAsTable(full_table_name)
            logging.info(f'Creating new table {full_table_name} from sdf with columns {sdf.columns}')        
        else:
            if upsert:

                # Create a temporary view of the new data
                sdf.createOrReplaceTempView("updates")

                # Construct the merge condition
                merge_condition = " AND ".join([f"target.{col} = updates.{col}" for col in merge_columns])

                merge_query = f"""
                MERGE INTO {full_table_name} AS target
                USING updates
                ON {merge_condition}
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """

                try:
                    # Enable automatic schema evolution
                    sdf.sparkSession.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                    
                    sdf.sparkSession.sql(merge_query)
                    logging.info(f'Upserted data into existing table {full_table_name}')
                finally:
                    # Disable automatic schema evolution
                    sdf.sparkSession.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
            else:
                sdf.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(full_table_name)

    except Exception as e:
        logging.error(f"An error occurred preparing table {full_table_name}: {e}")
        raise
        
def get_current_london_timestamp(return_only_date: bool = False) -> datetime.datetime | datetime.date:
    """
    Returns the current timestamp in London time.

    This function calculates the current timestamp in London time (UTC+1).

    Args:
        return_only_date: Optional[bool] = False: Default is False, but if True returns the current date in London instead of timestamp

    Returns:
        datetime.datetime | datetime.date: The current timestamp or date in London time.
    """

    fixed_utc_plus_one: datetime.timezone = datetime.timezone(datetime.timedelta(hours=1))
    current_time_utc_plus_one: datetime.datetime = datetime.datetime.now(fixed_utc_plus_one)

    if return_only_date:
        current_date_london = current_time_utc_plus_one.date()
        return current_date_london

    return current_time_utc_plus_one

        
def cast_all_columns_as_string(df: DataFrame) -> DataFrame:
    """
    Casts all columns in a DataFrame to string type.

    This function iterates through all columns in the input DataFrame and casts each column to string type.
    It returns a new DataFrame with all columns as string.

    Args:
        df(DataFrame): The DataFrame to be cast.

    Returns:
        DataFrame: A new DataFrame with all columns as string.
    """
    cast_expression: list = [F.expr(f"cast(`{col}` as string) as `{col}`") for col in df.columns]
    sdf: DataFrame = df.select(*cast_expression)
    return sdf


def order_columns_df(df: DataFrame, columns_first: list):
    """
    Reorders columns in a DataFrame.

    This function takes a DataFrame and a list of column names. It reorders the columns in the DataFrame so that
    the specified column names appear first, followed by the remaining columns in their original order.

    Args:
        df(DataFrame): The DataFrame to be reordered.
        columns_first(list): A list of column names to be placed at the beginning of the DataFrame.

    Returns:
        DataFrame: A new DataFrame with the columns reordered.

    Example:
        >>> df = spark.createDataFrame([('a', 'b', 'c'), ('d', 'e', 'f')], ['col1', 'col2', 'col3'])
        >>> order_columns_df(df, ['col2', 'col3'])
        DataFrame[col2: string, col3: string, col1: string]
    """
    
    remaining_columns: list = [col for col in df.columns if col not in columns_first]
    new_column_order: list = columns_first + remaining_columns

    # Select columns using the new order, properly handling special characters
    df_reordered: DataFrame = df.select(*new_column_order) 

    return df_reordered



class FileRecordStatus(Enum):
    INCOMPLETE = 'incomplete'
    COMPLETE = 'complete'
    FAILED = 'failed'


def get_client_landing_zone(customer_code: str) -> str:
    """
    Returns the raw landing volume path for a given customer code.

    This function constructs the path to the raw landing volume for a specific customer,
    using the provided customer code. The path follows the format:
    `/Volumes/landing_zone/client-{customer_code.lower()}/raw_files/`.

    Args:
        customer_code(str): The customer code for which to retrieve the landing volume path.

    Returns:
        str: The raw landing volume path for the specified customer.

    Example:
        >>> get_client_landing_zone('abc123')
        '/Volumes/landing_zone/client-ABC123/raw_files/'
    """
    raw_landing_volume_path = f'/Volumes/landing_zone/client-{customer_code.lower()}/raw_files/'
    return raw_landing_volume_path


def get_client_layer_location(layer_name: str, customer_code: str) -> str:
    """Returns the S3 location for a specific layer and customer.

    This function constructs the S3 location for a layer, using the provided layer name and customer code.
    The location follows the format:
    `s3://deus-{layer_name.lower()}-layer/client-{customer_code.lower()}`.

    Args:
        layer_name(str): The name of the layer for which to retrieve the S3 location.
        customer_code(str): The customer code for which to retrieve the S3 location.

    Returns:
        str: The S3 location for the specified layer and customer.

    Example:
        >>> get_client_layer_location('gold', 'xyz456')
        's3://deus-gold-layer/client-XYZ456'
    """

    layer_location: str = f"s3://deus-{layer_name.lower()}-layer"
    return f"{layer_location}/client-{customer_code.lower()}"


def initialize_job():
    """Initializes the Spark session and Dbutils.

    This function retrieves the Spark session instance and Dbutils, ensuring they are available for use within the job.

    Returns:
        tuple: A tuple containing the Spark session and Dbutils objects.
    """

    spark: SparkSession = SparkSessionSingleton.get_instance().get_spark_session()
    dbutils = get_dbutils()

    return spark, dbutils

def parse_task_params() -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Parse and validate task parameters.

    Raises:
        ValueError: If no task parameters are provided or if there is a JSON decode error.

    Returns:
        Tuple[Dict[str, Any], Dict[str, Any]]: A tuple containing client settings and validated job parameters.
    """

    if not sys.argv[1:]:  # Check if the string is empty
        raise ValueError("No task parameters provided.")

    try:
        task_params = json.loads(sys.argv[1:][0])
        client_settings: dict[str, Any] = task_params['client_settings']
        job_custom_params: dict[str, str] = {key: value for key, value in task_params.items() if key != 'client_settings'}

        logging.debug(f"job_custom_params {job_custom_params}")

        config = TaskJobParamConfig(**job_custom_params)
        job_custom_params_after_validation: dict[str, Any] = config.model_dump()

        logging.debug(f"job_custom_params_after_validation {job_custom_params_after_validation}")

        job_custom_params_after_validation = {k: v for k, v in job_custom_params_after_validation.items() if v is not None}

        logging.debug(f"job_custom_params_after_validation {job_custom_params_after_validation}")

        return client_settings, job_custom_params_after_validation
    
    except json.JSONDecodeError as e:
        raise ValueError(f"An error occurred parsing task params: {e}")

def get_all_files(data_type: str, location: str, dbutils) -> tuple[list[str], list[str]]:
    """
    Retrieve all files and directories of a specific data type from a given location in a databricks.

    Args:
        data_type (str): The file extension/type to filter by.
        location (str): The location to search for files.
        dbutils: The dbutils object for file system operations.

    Returns:
        tuple[list[str], list[str]]: A tuple containing a list of all filtered file paths and a list of all filtered directories.
    """

    all_compressed_dir = dbutils.fs.ls(location)

    for directory in all_compressed_dir:
        all_files_filtered = [file.path for file in dbutils.fs.ls(directory.path) if file.name.endswith(data_type)]
        all_dir_filtered = list(set("/".join(file.split("/")[:-1]) for file in all_files_filtered))

    return all_files_filtered, all_dir_filtered


def load_json(path: str) -> dict[str, Any]:
    """
    Load a JSON file from a given path.

    Args:
        path (str): The path to the JSON file.

    Raises:
        FileNotFoundError: If the file is not found.
        json.JSONDecodeError: If there is an error decoding the JSON file.

    Returns:
        dict[str, Any]: The loaded JSON data.
    """

    try:
        with open(path, 'r') as file:
            local_table_config = json.load(file)
        return local_table_config
    except FileNotFoundError:
        logging.error(f"File not found: {path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {path}")
        raise

def checksum(file_path):
    """
    Calculates the SHA256 checksum of a file.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The SHA256 checksum of the file.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def extract_zip_file(zip_file_path, extraction_folder):
    """
    Extracts the contents of a zip file to the specified extraction folder.

    Args:
        zip_file_path (str): The path to the zip file.
        extraction_folder (str): The folder where the contents of the zip file will be extracted.

    Returns:
        bool: True if the extraction is successful, False otherwise.
    """
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extraction_folder)
        return True
    except Exception as e:
        logging.error(f"Error extracting {zip_file_path}: {e}")
        return False
    
def parse_s3_path(s3_path):
    """
    Parses an S3 path and returns the bucket name and key.

    Args:
        s3_path (str): The S3 path to parse.

    Returns:
        tuple: A tuple containing the bucket name and key.

    Example:
        >>> parse_s3_path('s3://my-bucket/my-folder/my-file.txt')
        ('my-bucket', 'my-folder/my-file.txt')
    """
    parsed_url = urlparse(s3_path)
    bucket_name = parsed_url.netloc
    key = parsed_url.path.lstrip('/')
    return bucket_name, key

def copy_zip_file(s3_path, dest_path):
    """
    Copy a zip file from an S3 bucket to a local path.

    Args:
        s3_path (str): The S3 path of the zip file in the format 'bucket_name/key'.
        dest_path (str): The local path where the zip file should be copied to.

    Returns:
        bool: True if the file was successfully copied, False otherwise.
    """
    try:
        # Use boto3 to copy the file from S3 to the local path
        s3 = boto3.client('s3')
        bucket_name, key = parse_s3_path(s3_path)
        s3.download_file(bucket_name, key, dest_path)
        logging.info(f"Successfully copied {s3_path} to {dest_path}")
        return True
    except Exception as e:
        logging.error(f"Error copying {s3_path} to {dest_path}: {e}")
        return False
    


if __name__ == '__main__':

    #test util functions here

    current_date_utc_plus_one = get_current_london_timestamp(return_only_date = True)
    default = "2024-06-24"
    default_date = datetime.datetime.strptime(default, "%Y-%m-%d").date()
    if current_date_utc_plus_one == default_date:
        print("equal")
    else:
        print("not equal")