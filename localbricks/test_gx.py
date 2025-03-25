from localbricks_factory import data_loader_manager
from deus_lib.great_expectations.dq_factory import DQFunctionSpec, DQFactory
import os
import yaml

from localbricks_factory import data_loader_manager
df = data_loader_manager(action= 'load_directly', share = 'localbricks', schema_table_name= "client-tui.flight_crew_data")

# Define data quality functions to test
columns_dq_validations = [
    DQFunctionSpec(
        function="expect_column_values_to_match_regex",
        args={"column": "ingestion_datetime", "regex": r"^[a-zA-Z ]*$"}),
    DQFunctionSpec(
        function="expect_column_values_to_match_regex",
        args={"column": "ingestion_job_run_id", "regex": r"^[a-zA-Z ]*$"})
        ]# flag with critical can be added

cwd = os.path.dirname(os.path.abspath(__name__))

# Initialize the DQFactory with parameters
dq_factory = DQFactory(
    industry="test_industry",
    table_full_name="test_catalog.test_schema.test_table",
    columns_dq_validations=columns_dq_validations,
    primary_key=["source_file_name"],
    local_fs_root_dir = cwd,
    spark = df.sparkSession
)

# Run the data quality process
results = dq_factory.run_dq_process(df)
checkpoint_result_dict = results.to_json_dict()
list_validation_results = []
for list_validation_result in results.list_validation_results():
    list_validation_results.append(list_validation_result.to_json_dict())

print(list_validation_results)
