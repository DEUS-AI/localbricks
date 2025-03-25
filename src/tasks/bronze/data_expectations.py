from deus_lib.great_expectations.dq_factory import DQFunctionSpec, DQFactory
import os
import yaml

def __read_yaml_to_dq_validations(file_path):
    with open(file_path, 'r') as file:
        yaml_data = yaml.safe_load(file)

    all_validations = {}

    for table in yaml_data['tables']:
        table_name = table['name']
        pk = table['pk']
        industry = table['industry']
        columns_dq_validations = []

        for validation in table['columns_dq_validations']:
            dq_spec = DQFunctionSpec(
                function=validation['function'],
                args=validation['args']
            )
            columns_dq_validations.append(dq_spec)

        all_validations[table_name] = [columns_dq_validations,pk, industry]

    return all_validations

def __get_columns(columns_dq_validations):

    columns = []
    for column_dq_validation in columns_dq_validations:
        columns.append(column_dq_validation.args.get('column', ''))

    return columns

def main():
    
    dq_yml_filename = "data_quality.yml"
    dq_yml_path = os.path.dirname(os.path.abspath(__name__)).replace("/src/tasks/bronze","/data_quality_clients/") + dq_yml_filename
    all_validations = __read_yaml_to_dq_validations(file_path= dq_yml_path)

    for table_name, (columns_dq_validations, pk, industry) in all_validations.items():

        columns = __get_columns(columns_dq_validations)
        columns.extend(pk)
        df = spark.sql(f"SELECT {','.join(columns)} FROM {table_name}")

        dq_factory = DQFactory(
            industry= industry,
            table_full_name=table_name,
            columns_dq_validations=columns_dq_validations,
            primary_key=pk,
            spark = df.sparkSession
            )
        
        source_df_after_gx = dq_factory.run_dq_process(df = df)

        source_df_after_gx.show()


if __name__ == '__main__':
    main()

