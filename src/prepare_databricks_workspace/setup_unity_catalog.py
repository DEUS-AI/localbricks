# Databricks notebook source
# MAGIC %md
# MAGIC Creating Catalogs

# COMMAND ----------

import os

ws_env = 'qa' # dev/qa/production
ws_storage_credentials = 'deus-test-uc-access'

current_dir = f"file:{os.getcwd()}"
env_config_file = f"{current_dir}/config_s3_external_location.{ws_env}.json"

deus_clients_s3_external_conn_config = spark.read.option("multiLine", "true").format("json").load(env_config_file)
deus_clients_s3_external_conn_config.show()

# COMMAND ----------

def create_landing_zone(ws_env: str, metastore_industry: str, client_code: str, s3_bucket: str):
    client_id = f'client-{client_code}'
    industry_landing_zone = f'{metastore_industry}__landing_zone__{ws_env}'
    
    # external location (s3)
    external_location_sql_query = f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `raw-layer-access-{ws_env}-{client_code}`
        URL '{s3_bucket}'
        WITH (STORAGE CREDENTIAL `{ws_storage_credentials}`)
        COMMENT 'creating access to external location on S3 for {client_id}';
    """
    spark.sql(external_location_sql_query)

    # catalog
    catalog_landing_zone_sql_query = f"""
        CREATE CATALOG IF NOT EXISTS `{industry_landing_zone}`;
    """
    spark.sql(catalog_landing_zone_sql_query)
    
    # schema + volume for /raw-files
    schemas_landing_zone_sql_query = f"""
        CREATE SCHEMA IF NOT EXISTS `{industry_landing_zone}`.`{client_id}`;
    """
    spark.sql(schemas_landing_zone_sql_query)
    
    external_landing_zone_volumes_sql_query = f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS `{industry_landing_zone}`.`{client_id}`.raw_files
        LOCATION '{s3_bucket}';
    """
    spark.sql(external_landing_zone_volumes_sql_query)
    
    # landing table for raw files process tracking
    raw_file_processing_table_sql_query = f"""
        CREATE TABLE IF NOT EXISTS `{industry_landing_zone}`.`{client_id}`.file_processing_table (
            filename STRING,
            filepath STRING,
            format STRING,
            status STRING,
            etag STRING,
            last_modified TIMESTAMP,
            size BIGINT
        );
    """
    spark.sql(raw_file_processing_table_sql_query)

def create_medallion_layer(ws_env: str, metastore_industry: str, client_code: str, layer_name: str):
    client_id = f'client-{client_code}'
    s3_bucket_layer = f's3://deus-medallion-databricks-{ws_env}/{layer_name}' # isolate physical data from diff environments (workspaces)
    industry_catalog_layer = f'{metastore_industry}__{layer_name}__{ws_env}'
    
    external_location_sql_query = f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{layer_name}-layer-access-{ws_env}`
        URL '{s3_bucket_layer}'
        WITH (STORAGE CREDENTIAL `{ws_storage_credentials}`)
        COMMENT 'creating access to external location on S3 for layer {layer_name}';
    """
    spark.sql(external_location_sql_query)
    
    catalog_layer_sql_query = f"""
        CREATE CATALOG IF NOT EXISTS `{industry_catalog_layer}`;
    """
    spark.sql(catalog_layer_sql_query)
    
    schemas_managed_location_layer_sql_query = f"""
        CREATE SCHEMA IF NOT EXISTS `{industry_catalog_layer}`.`{client_id}`
        MANAGED LOCATION '{s3_bucket_layer}/{client_id}';
    """
    spark.sql(schemas_managed_location_layer_sql_query)


# COMMAND ----------

# configure deus clients
for c in deus_clients_s3_external_conn_config.collect():
    client_code = str(c['client_code'])
    client_industry = str(c['industry'])
    client_s3_bucket = str(c['s3_bucket']).replace("{{WS_ENV}}", ws_env).replace("{{CLIENT_CODE}}", client_code).lower()

    create_landing_zone(ws_env=ws_env, metastore_industry=client_industry, client_code=client_code, s3_bucket=client_s3_bucket)
    create_medallion_layer(ws_env=ws_env, metastore_industry=client_industry, client_code=client_code, layer_name='bronze')
    create_medallion_layer(ws_env=ws_env, metastore_industry=client_industry, client_code=client_code, layer_name='silver')
    create_medallion_layer(ws_env=ws_env, metastore_industry=client_industry, client_code=client_code, layer_name='gold')
    
    # IMPORTANT: make sure to bind resources to the corresponding isolated env workspace
    # Follow this: 
    # https://docs.databricks.com/en/catalogs/binding.html#bind-a-catalog-to-one-or-more-workspaces
    # https://docs.databricks.com/en/connect/unity-catalog/external-locations.html#bind-an-external-location-to-one-or-more-workspaces


# COMMAND ----------

