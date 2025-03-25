from pyspark.sql import SparkSession
from itertools import islice
import great_expectations as gx
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context.types.base import (
    FilesystemStoreBackendDefaults,
    AnonymizedUsageStatisticsConfig,
    DataContextConfig
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context import EphemeralDataContext
from pyspark.sql import DataFrame
from enum import Enum
import datetime
from typing import Union, Any, Optional
import os
from json import dumps, loads
from dataclasses import dataclass
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)
import logging 
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    col,
    dayofmonth,
    explode,
    from_json,
    lit,
    month,
    schema_of_json,
    struct,
    to_json,
    to_timestamp,
    transform,
    year,
    first,
    collect_set,
    when
)
from pyspark.sql.types import StringType
from deus_lib.utils.common import write_table
from pyspark.sql.types import StringType, StructType, MapType


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create file handler
# file_handler = logging.FileHandler(f'/opt/deus_dev_env/localbricks_runs/dq_factory.log')
# file_handler.setLevel(logging.INFO)

# # Create formatter and add it to the handler
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(formatter)

# # Add the file handler to the logger
# logger.addHandler(file_handler)

class DQCheckpointsResultsException(Exception):
    """Exception for when the checkpoint results parsing fail."""

    pass

class DQValidationsFailedException(Exception):
    """Exception for when the data quality validations fail."""

    pass

class DQExpectationException(Exception):
    """Custom exception for Data Quality expectation failures."""
    pass

@dataclass
class DQFunctionSpec(object):
    """Defines a data quality function specification.

    - function - name of the data quality function (expectation) to execute.
    It follows the great_expectations api https://greatexpectations.io/expectations/.
    - args - args of the function (expectation). Follow the same api as above.
    """

    function: str
    args: Optional[dict] = None

class DQDefaults(Enum):

    CUSTOM_EXPECTATION_LIST = [
        "expect_column_values_to_be_json_parseable"
    ]
    S3_BACKEND_BUCKET = 'deus-data-quality'
    DATASOURCE_CLASS_NAME = "Datasource"
    DATASOURCE_EXECUTION_ENGINE = "SparkDFExecutionEngine"
    DATA_CONNECTORS_MODULE_NAME = "great_expectations.datasource.data_connector"
    DATA_CONNECTORS_CLASS_NAME = "RuntimeDataConnector"
    DQ_BATCH_IDENTIFIERS = ["catalog", "schema","table","timestamp"]
    DATA_CHECKPOINTS_CLASS_NAME = "SimpleCheckpoint"
    DATA_CHECKPOINTS_CONFIG_VERSION = 1.0
    GX_RESULT_FORMAT = 'COMPLETE'
    VALIDATION_COLUMN_IDENTIFIER = "validationresultidentifier"
    DQ_VALIDATIONS_SCHEMA = StructType(
        [
            StructField(
                "dq_validations",
                StructType(
                    [
                        StructField("run_name", StringType()),
                        StructField("run_success", BooleanType()),
                        StructField("run_row_success", BooleanType()),
                        StructField(
                            "dq_failure_details",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("expectation_type", StringType()),
                                    ]
                                ),
                            ),
                        ),
                    ]
                ),
            )
        ]
    )

class DQFactory():

    def __init__(self, industry: str, table_full_name: str, columns_dq_validations: dict[str, str], primary_key: list[str], local_fs_root_dir: Optional[str] = None, spark: SparkSession = None):

        self.catalog, self.schema, self.table_name = [part.replace('`', '') for part in table_full_name.lower().split('.')]
        self.industry = industry
        self.columns_dq_validations: list[DQFunctionSpec] = columns_dq_validations
        self.datetime = self.london_datetime()
        self.primary_key = primary_key
        self.local_fs_root_dir = local_fs_root_dir or os.path.dirname(os.path.abspath(__file__))
        self.spark = spark
        logger.info(f"DQFactory initialized with catalog={self.catalog}, schema={self.schema}, table_name={self.table_name}")

    @staticmethod
    def london_datetime():
        fixed_utc_plus_one = datetime.timezone(datetime.timedelta(hours=1))
        current_time_utc_plus_one = datetime.datetime.now(fixed_utc_plus_one).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Current London datetime: {current_time_utc_plus_one}")
        return current_time_utc_plus_one        
    
    @staticmethod
    def __is_databricks():
        is_databricks = 'DATABRICKS_RUNTIME_VERSION' in os.environ
        logger.info(f"Running in Databricks: {is_databricks}")
        return is_databricks
    
    @staticmethod
    def __get_data_docs_sites(site_name, data_docs_site, data_docs_local_fs: str = None):

        data_docs_site[site_name]["show_how_to_buttons"] = False

        if site_name == "local_site":
            data_docs_site[site_name]["store_backend"][
                "base_directory"
            ] = "data_docs/site/"

            if data_docs_local_fs:
                # Enable to write data_docs in a separated path
                data_docs_site[site_name]["store_backend"][
                    "root_directory"
                ] = data_docs_local_fs

        logger.info(f"Configured data docs site for {site_name}")

        return data_docs_site
    
    @classmethod
    def __get_data_context_config(cls, catalog: str, schema: str, local_fs_root_dir: str, spark: SparkSession, data_docs_local_fs: Optional[str] = None) -> DataContextConfig:
        store_backend: FilesystemStoreBackendDefaults
        data_docs_site = None

        if cls.__is_databricks():

            client_code = schema.split('-')[-1]
            spark.sql(f"""CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.`{schema}`.data_quality
                                LOCATION 's3://deus-bronze-layer/client-{client_code.upper()}/data_quality'""")
                
            print(f'Saving context into volume /context/')
            store_backend = FilesystemStoreBackendDefaults(
                root_directory=f'/Volumes/bronze_layer/{schema}/data_quality' # to change and make it dynamic based on docker probably
            )
            data_docs_site = cls.__get_data_docs_sites(
                "local_site", store_backend.data_docs_sites, data_docs_local_fs= data_docs_local_fs
            )

        else:
            print(f'Saving context locally into {local_fs_root_dir}/context/')
            store_backend = FilesystemStoreBackendDefaults(
                root_directory=f'{local_fs_root_dir}/gx/context/' # to change and make it dynamic based on docker probably
            )
            data_docs_site = cls.__get_data_docs_sites(
                "local_site", store_backend.data_docs_sites, data_docs_local_fs= data_docs_local_fs
            )
            
        config = DataContextConfig(
            store_backend_defaults=store_backend,
            data_docs_sites=data_docs_site,
            anonymous_usage_statistics=AnonymizedUsageStatisticsConfig(enabled=False),
        )
        logger.info(f"DataContextConfig created with store backend: {store_backend}")
        return config
    
    @classmethod
    def build_data_docs(
        cls,
        local_fs_root_dir: str,
        catalog: str = None,
        schema: str = None,
        data_docs_local_fs: str = None
    ) -> None:
        """Build Data Docs for the project."""

        # Get the DataContextConfig using the class method
        context_config = cls.__get_data_context_config(catalog=catalog, schema=schema, local_fs_root_dir=local_fs_root_dir, data_docs_local_fs= data_docs_local_fs)

        # Get the DataContext
        context = gx.get_context(project_config=context_config)

        # Build the data docs
        context.build_data_docs()


    def __get_data_source_defaults(self) -> dict[str, Any]:
        
        defaults =  {
            "name": f"{self.industry}-{self.catalog}-{self.schema}-{self.table_name}-datasource",
            "class_name": DQDefaults.DATASOURCE_CLASS_NAME.value,
            "execution_engine": {
                "class_name": DQDefaults.DATASOURCE_EXECUTION_ENGINE.value,
                "persist": False,
            },
            "data_connectors": {
                f"{self.industry}-{self.catalog}-{self.schema}-{self.table_name}-data_connector": {
                    "module_name": DQDefaults.DATA_CONNECTORS_MODULE_NAME.value,
                    "class_name": DQDefaults.DATA_CONNECTORS_CLASS_NAME.value,
                    "assets": {
                       f"{self.industry}-{self.catalog}-{self.schema}-{self.table_name}": {"batch_identifiers": DQDefaults.DQ_BATCH_IDENTIFIERS.value
                        }
                    },
                }
            },
        }
        logger.info(f"Data source defaults: {defaults}")
        return defaults

    def __get_batch_request(self, df: DataFrame) -> RuntimeBatchRequest:
            
        batch_request = RuntimeBatchRequest(
            datasource_name=f"{self.industry}-{self.catalog}-{self.schema}-{self.table_name}-datasource",
            data_connector_name=f"{self.industry}-{self.catalog}-{self.schema}-{self.table_name}-data_connector",
            data_asset_name= f"{self.industry}-{self.catalog}-{self.schema}-{self.table_name}", 
            batch_identifiers={
                "catalog": self.catalog,
                "schema": self.schema,
                "table": self.table_name,
                "timestamp": self.datetime,
            },
            runtime_parameters={"batch_data": df},
        )     

        logger.info(f"Batch request created: {batch_request}")
        return batch_request
    

    def __get_run_dq_validator(self, context: BaseDataContext, batch_request: RuntimeBatchRequest, expectation_suite_name: str):
        
        validator = context.get_validator(
            batch_request= batch_request, expectation_suite_name= expectation_suite_name
        )

        if self.columns_dq_validations:
            for dq_function in self.columns_dq_validations:
                 kwargs = dq_function.args if dq_function.args else {}
                 kwargs['catch_exceptions'] = True
                 getattr(validator, dq_function.function)(
                    **kwargs
                )
        
        logger.info(f"Validator: {validator}")
        
        validator.save_expectation_suite(discard_failed_expectations=False)
    
    def __get_unexpected_rows_pk(self) -> list[str]:

        if self.primary_key:
            return self.primary_key

    def __transform_checkpoint_results(self, checkpoint_results: dict[str, Any]) -> DataFrame:
        results_json_dict = loads(dumps(checkpoint_results))

        results_dict = {}
        for key, value in results_json_dict.items():
            if key == "run_results":
                checkpoint_result_identifier = list(value.keys())[0]
                if (
                    str(checkpoint_result_identifier)
                    .lower()
                    .startswith(DQDefaults.VALIDATION_COLUMN_IDENTIFIER.value)
                ):
                    results_dict["validation_result_identifier"] = checkpoint_result_identifier
                    results_dict["run_results"] = value[checkpoint_result_identifier]
                else:
                    raise DQCheckpointsResultsException(
                        """The checkpoint result identifier format is not in accordance to what is expected"""
                    )
            else:
                results_dict[key] = value

        logger.info(f"results_dict is {results_dict}")

        # Process the results in chunks
        chunk_size = 1000  # Adjust this value based on your data size and available memory
        df = self.__process_results_in_chunks(results_dict, chunk_size)

        cols_to_expand = ["run_id"]
        df = (
            df.select(
                [col(c) if c not in cols_to_expand else col(f"{c}.*") for c in df.columns]
            )
            .drop(*cols_to_expand)
            .withColumn("catalog", lit(self.catalog))
            .withColumn("schema", lit(self.schema))
            .withColumn("table_name", lit(self.table_name))
        )

        logger.info(f"Columns before explode: {df.columns}")

        df = self.__explode_results(df)
        return df

    def __process_results_in_chunks(self, results_dict: dict, chunk_size: int) -> DataFrame:
        def chunk_dict(d, size):
            it = iter(d)
            for i in range(0, len(d), size):
                yield {k: d[k] for k in islice(it, size)}

        dfs = []
        for chunk in chunk_dict(results_dict, chunk_size):
            # Create a DataFrame for this chunk
            chunk_df = self.spark.createDataFrame([(json.dumps(chunk),)], ["json_data"])
            
            # Use a flexible schema to parse the JSON
            flexible_schema = MapType(StringType(), StringType())
            chunk_df = chunk_df.withColumn("parsed", from_json(col("json_data"), flexible_schema))
            
            # Flatten the structure
            for field in chunk_df.select("parsed.*").columns:
                chunk_df = chunk_df.withColumn(field, col(f"parsed.`{field}`"))
            
            # Drop the original columns
            chunk_df = chunk_df.drop("json_data", "parsed")
            
            # Handle potential nested structures
            for column in chunk_df.columns:
                if isinstance(chunk_df.schema[column].dataType, StringType):
                    try:
                        # Try to parse as JSON
                        parsed_df = chunk_df.withColumn(f"{column}_parsed", from_json(col(column), MapType(StringType(), StringType())))
                        if parsed_df.filter(col(f"{column}_parsed").isNotNull()).count() > 0:
                            # If successful, replace the original column
                            chunk_df = parsed_df.drop(column).withColumnRenamed(f"{column}_parsed", column)
                    except:
                        # If parsing fails, keep the original column
                        pass
            
            dfs.append(chunk_df)

        # Combine all chunk DataFrames
        if len(dfs) > 1:
            return dfs[0].unionByName(*dfs[1:], allowMissingColumns=True)
        else:
            return dfs[0]
    
    def __explode_results(self,
        df: DataFrame,
    ) -> DataFrame:
        """Transform dq results dataframe exploding a set of columns.

        Args:
            df: dataframe with dq results to be exploded.
        """
        df = df.withColumn(
            "validation_results", explode("run_results.validation_result.results")
        )

        new_columns = [
            "validation_results.expectation_config.kwargs.*",
            "run_results.validation_result.statistics.*",
            "validation_results.expectation_config.expectation_type",
            "validation_results.success as expectation_success",
            "validation_results.exception_info"
        ]

        df_exploded = df.selectExpr(*df.columns, *new_columns).drop(
            *[c.replace(".*", "").split(" as", maxsplit=1)[0] for c in new_columns]
        )

        schema = df_exploded.schema.simpleString()

        logger.info(f"Current columns: {df_exploded.columns}")
        if "unexpected_index_list" in schema:
            df_exploded = (
                df_exploded.withColumn(
                    "unexpected_index_list",
                    array(struct(lit(True).alias("run_success"))),
                )
                if df.select(
                    col("validation_results.result.unexpected_index_list")
                ).dtypes[0][1]
                == "array<string>"
                else df_exploded.withColumn(
                    "unexpected_index_list",
                    transform(
                        col("validation_results.result.unexpected_index_list"),
                        lambda x: x.withField("run_success", lit(False)),
                    ),
                )
            )

        if "observed_value" in schema:
            df_exploded = df_exploded.withColumn(
                "observed_value", col("validation_results.result.observed_value")
            )

        final_df = (df_exploded.withColumn("run_time_year", year(to_timestamp("run_time")))
                    .withColumn("run_time_month", month(to_timestamp("run_time")))
                    .withColumn("run_time_day", dayofmonth(to_timestamp("run_time")))
                    .withColumn("checkpoint_config", to_json(col("checkpoint_config")))
                    .withColumn("run_results", to_json(col("run_results")))
                    .withColumn(
                        "kwargs", to_json(col("validation_results.expectation_config.kwargs"))
                    )
                    .withColumn("validation_results", to_json(col("validation_results"))))
        
        # Drop unnecessary columns Can be used later if needed
        base_columns = [
            "run_name", "run_time", "catalog", "schema", "table_name", "success",
            "column", "expectation_type", "expectation_success",
            "evaluated_expectations", "success_percent", "successful_expectations", 
            "unsuccessful_expectations", "batch_id", "exception_info","run_time_year", "run_time_month", "run_time_day"
        ]

        # Add unexpected_index_list only if it contains non-null values
        if "unexpected_index_list" in final_df.columns and final_df.filter(col("unexpected_index_list").isNotNull()).count() > 0:
            base_columns.append("unexpected_index_list")
        
        # Add observed_value only if it exists and contains non-null values
        if "observed_value" in final_df.columns and final_df.filter(col("observed_value").isNotNull()).count() > 0:
            base_columns.append("observed_value")
        
        final_df = final_df.select(*base_columns)

        logger.info(f"Columns of the final dataframe: {final_df.columns}")
    
        return final_df
    
    def __write_result_df(self, df: DataFrame) -> None:
        
        if self.__is_databricks():
            write_table(sdf = df, full_table_name = f"{self.catalog}.{self.schema}.dq_results", cluster_keys= [self.table_name])
        else:
            output_dir = f"{self.local_fs_root_dir}/dq_results_local_tests"
            os.makedirs(output_dir, exist_ok=True)
            df.toPandas().to_csv(f"{output_dir}/{self.catalog}_{self.schema}_{self.table_name}.csv", index = False)

    def __check_critical_functions_metadata(self, failed_expectations: list[Any]) -> list:
        critical_failure = []

        for expectation in failed_expectations:
            meta = expectation["meta"]
            if meta and "critical" in meta.keys():
                critical_failure.append(expectation["expectation_type"])

        return critical_failure
    
    def __log_or_fail(self, results: CheckpointResult) -> None:
        """Log the execution of the Data Quality process.

        Args:
            results: the results of the DQ process.
            dq_spec: data quality specification.
        """
        if results["success"]:
            logger.info(
                "The data passed all the expectations defined. Everything looks good!"
            )
        else:
            failed_expectations = self.__get_failed_expectations(results)
            critical_failure = self.__check_critical_functions_metadata(
                failed_expectations
            )

            if critical_failure:
                raise DQValidationsFailedException(
                    f"Data Quality Validations Failed, the following critical "
                    f"expectations failed: {critical_failure}."
                )
                
        return results

    def __get_failed_expectations(self, results: CheckpointResult) -> list[Any]:
        """Get the failed expectations of a Checkpoint result."""
        failed_expectations = []
        for validation_result in results.list_validation_results():
            expectations_results = validation_result["results"]
            for result in expectations_results:
                if not result["success"]:
                    failed_expectations.append(result["expectation_config"])
                    exception_info = result.get("exception_info", {})
                    if exception_info:
                        for exception_key, exception_details in exception_info.items():
                            
                            if isinstance(exception_details, dict):
                                raised_exception = exception_details.get("raised_exception", False)
                                exception_message = exception_details.get("exception_message", "No message provided")
                            else:
                                raised_exception= False
                            
                            if raised_exception:
                                error_message = f"""The expectation {str(result["expectation_config"])}
                                raised the following exception:
                                {exception_message}"""
                                logger.error(error_message)
                                raise DQExpectationException(error_message)

            logger.error(
                f"{len(failed_expectations)} out of {len(expectations_results)} "
                f"Data Quality Expectation(s) have failed! Failed Expectations: "
                f"{failed_expectations}"
            )
        return failed_expectations

    def __configure_and_run_checkpoint(self, context: EphemeralDataContext, batch_request: RuntimeBatchRequest, expectation_suite_name: str, source_pk: list[str]) -> tuple[CheckpointResult, DataFrame]:

        checkpoint_name = f"{self.catalog}-{self.schema}-{self.table_name}-checkpoint"
        context.add_or_update_checkpoint(
            name=checkpoint_name,
            class_name=DQDefaults.DATA_CHECKPOINTS_CLASS_NAME.value,
            config_version=DQDefaults.DATA_CHECKPOINTS_CONFIG_VERSION.value,
            run_name_template=f"%Y%m%d-%H%M%S-{checkpoint_name}",
        )

        result_format: dict[str, Any] = {
            "result_format": DQDefaults.GX_RESULT_FORMAT.value,
        }
        if source_pk:
            result_format = {
                **result_format,
                "unexpected_index_column_names": source_pk,
            }

        results = context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": expectation_suite_name,
                }
            ],
            result_format=result_format,
        )

        logger.info(f"Results are: {results}")

        results = self.__log_or_fail(results = results)

        results_df = self.__transform_checkpoint_results(results.to_json_dict())

        return results, results_df
    

    @staticmethod
    def __get_row_tagged_fail_df(
        failures_df: DataFrame,
        source_df: DataFrame,
        source_pk: list[str],
    ) -> DataFrame:
        """Get the source_df DataFrame tagged with the row level failures.

        Args:
            failures_df: dataframe having all failed expectations from the DQ execution.
            raised_exceptions: whether there was at least one expectation raising
                exceptions (True) or not (False).
            source_df: the source dataframe being tagged with DQ results.
            source_pk: the primary key of the source data.

        Returns: the source_df tagged with the row level failures.
        """
        if "unexpected_index_list" in failures_df.schema.simpleString():
            row_failures_df = (
                failures_df.alias("a")
                .withColumn("exploded_list", explode(col("unexpected_index_list")))
                .selectExpr("a.*", "exploded_list.*")
                .groupBy(*source_pk)
                .agg(
                    struct(
                        first(col("run_name")).alias("run_name"),
                        first(col("success")).alias("run_success"),
                        first(col("expectation_success")).alias("run_row_success"),
                        collect_set(
                            struct(
                                col("expectation_type"),
                            )
                        ).alias("dq_failure_details"),
                    ).alias("dq_validations")
                )
            )

            if all(item in row_failures_df.columns for item in source_pk):
                join_cond = [
                    col(f"a.{key}").eqNullSafe(col(f"b.{key}")) for key in source_pk
                ]
                source_df = (
                    source_df.alias("a")
                    .join(row_failures_df.alias("b"), join_cond, "left")
                    .select("a.*", "b.dq_validations")
                )

        return source_df
    
    @staticmethod
    def _join_complementary_data(
        run_name: str, run_success: bool, source_df: DataFrame
    ) -> DataFrame:
        """Join the source_df DataFrame with complementary data.

        The source_df was already tagged/joined with the row level DQ failures, in case
        there were any. However, there might be cases for which we don't have any
        failure (everything succeeded) or cases for which only not row level failures
        happened (e.g. table level expectations or column level aggregations), and, for
        those we need to join the source_df with complementary data.

        Args:
            run_name: the name of the DQ execution in great expectations.
            run_success: whether the general execution of the DQ was succeeded (True)
                or not (False).
            source_df: the source dataframe being tagged with DQ results.

        Returns: the source_df tagged with complementary data.
        """
        complementary_data = [
            {
                "dq_validations": {
                    "run_name": run_name,
                    "run_success": run_success,
                    "run_row_success": True,
                }
            }
        ]
        complementary_df = source_df.sparkSession.createDataFrame(
            complementary_data, schema=DQDefaults.DQ_VALIDATIONS_SCHEMA.value
        )

        return (
            source_df.crossJoin(
                complementary_df.withColumnRenamed(
                    "dq_validations", "tmp_dq_validations"
                )
            )
            .withColumn(
                "dq_validations",
                when(
                    col("dq_validations").isNotNull(), col("dq_validations")
                ).otherwise(col("tmp_dq_validations"))
                if "dq_validations" in source_df.columns
                else col("tmp_dq_validations"),
            )
            .drop("tmp_dq_validations")
        )

    
    def __tag_source_with_dq(self, source_pk, source_df, results_df):

        run_name = results_df.select("run_name").first()[0]
        run_success = True
        
        failures_df = (
            results_df.filter(
                "expectation_success == False and size(unexpected_index_list) > 0"
            )
            if "unexpected_index_list" in results_df.schema.simpleString()
            else results_df.filter("expectation_success == False")
        )

        if failures_df.isEmpty() is not True:
            run_success = False

            source_df = self.__get_row_tagged_fail_df(
                failures_df, source_df, source_pk
            )

        return self._join_complementary_data(
            run_name, run_success, source_df
        )
    
    def run_dq_process(self, df: DataFrame):

        # for expectation in DQDefaults.CUSTOM_EXPECTATION_LIST.value:
        #     importlib.__import__(
        #         "lakehouse_engine.dq_processors.custom_expectations." + expectation
        #     )

        context = gx.get_context(project_config=self.__get_data_context_config(catalog = self.catalog, schema = self.schema, spark = self.spark, local_fs_root_dir = self.local_fs_root_dir, data_docs_local_fs= None))

        context.add_datasource(**self.__get_data_source_defaults())

        expectation_suite_name = (
            f"{self.catalog}-{self.schema}-{self.table_name}"
        )
        context.add_or_update_expectation_suite(
            expectation_suite_name= expectation_suite_name
        )

        batch_request = self.__get_batch_request(df = df)

        self.__get_run_dq_validator(context= context, batch_request= batch_request, expectation_suite_name= expectation_suite_name)

        source_pk = self.__get_unexpected_rows_pk()

        results, results_df = self.__configure_and_run_checkpoint(context= context, batch_request= batch_request, expectation_suite_name= expectation_suite_name, source_pk= source_pk)

        self.__write_result_df(df = results_df)

        source_data_after_gx = self.__tag_source_with_dq(source_pk, df, results_df)

        logging.info(results)
        
        return source_data_after_gx