from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, length, to_date, expr, upper
from deus_lib.utils.sparksession import SparkSessionSingleton

spark = SparkSessionSingleton.get_instance().spark

def calculate_imo(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    df = df.withColumn('vehicle_upper', col('vehicle').upper())
    
    state_df = spark.createDataFrame([(k.upper(), v) for k, v in state.items()], ['ship', 'imo'])

    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumn('imo', col('imo')).drop('vehicle_upper', 'ship')

    return df

def calculate_origin(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    df = df.withColumn('origin', when(length(col('origin')) >= 3, col('origin')).otherwise(None))
    return df

def calculate_destination(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    df = df.withColumn('destination', when(length(col('destination')) >= 3, col('destination')).otherwise(None))
    return df

def calculate_arrival_datetime(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    df = df.withColumn('arrival_datetime', to_date(col('report_date')))
    return df

def calculate_ocurrence_datetime(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    df = df.withColumn('arrival_datetime', to_date(col('report_date')))
    df = df.withColumn('ocurrence_datetime', expr("arrival_datetime - interval 1 day"))
    return df

def create_lookup_dataframe(state: dict):
    return spark.createDataFrame([(k.upper(), v) for k, v in state.items()], ['ship', 'value'])

def calculate_aux_engine1_max_load(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aux_engine1_max_load').drop('vehicle_upper', 'ship')
    return df

def calculate_aux_engine1_min_load(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aux_engine1_min_load').drop('vehicle_upper', 'ship')
    return df

def calculate_aux_engine2_max_load(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aux_engine2_max_load').drop('vehicle_upper', 'ship')
    return df

def calculate_aux_engine2_min_load(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aux_engine2_min_load').drop('vehicle_upper', 'ship')
    return df

def calculate_aux_engine3_max_load(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aux_engine3_max_load').drop('vehicle_upper', 'ship')
    return df

def calculate_aux_engine3_min_load(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aux_engine3_min_load').drop('vehicle_upper', 'ship')
    return df

def calculate_aux_engine4_max_load(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aux_engine4_max_load').drop('vehicle_upper', 'ship')
    return df

def calculate_aux_engine4_min_load(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aux_engine4_min_load').drop('vehicle_upper', 'ship')
    return df

def calculate_aeu_lower_threshold(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'aeu_lower_threshold').drop('vehicle_upper', 'ship')
    return df

def calculate_emo_delta_fuel(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'emo_delta_fuel').drop('vehicle_upper', 'ship')
    return df

def calculate_vessel_type(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'vessel_type').drop('vehicle_upper', 'ship')
    df = df.filter(col('vessel_type').isNotNull())
    return df

def calculate_vehicle_email_operator1(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'vehicle_email_operator1').drop('vehicle_upper', 'ship')
    return df

def calculate_vehicle_email_operator2(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'vehicle_email_operator2').drop('vehicle_upper', 'ship')
    return df

def calculate_team_name(df: DataFrame, state: dict, field_type: str) -> DataFrame:
    state_df = create_lookup_dataframe(state)
    df = df.withColumn('vehicle_upper', upper(col('vehicle')))
    df = df.join(state_df, df.vehicle_upper == state_df.ship, 'left')
    df = df.withColumnRenamed('value', 'team_name').drop('vehicle_upper', 'ship')
    return df