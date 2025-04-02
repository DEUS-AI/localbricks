import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import Any

def calculate_identifier(df: DataFrame, state: dict[str, Any], field_type: str):
    return df.withColumn('identifier', 
                       F.concat(F.col('imo'), 
                                F.lit('-'), 
                                F.col('report_date_utc')).cast(field_type))

def calculate_origin(df: DataFrame, state: dict[str, Any], field_type: str):
    return df.withColumn('origin', 
                       F.when(F.col('origin').isNull(), F.lit('UNKNOWN'))
                        .otherwise(F.col('origin'))
                        .cast(field_type))

def calculate_destination(df: DataFrame, state: dict[str, Any], field_type: str):
    return df.withColumn('destination', 
                       F.when(F.col('destination').isNull(), F.lit('UNKNOWN'))
                        .otherwise(F.col('destination'))
                        .cast(field_type))

def calculate_voyage_time_hs(df: DataFrame, state: dict[str, Any], field_type: str):
    return df.withColumn('voyage_time_hs', 
                       F.when(F.col('log_duration').isNotNull(), F.col('log_duration'))
                        .otherwise(F.lit(24))
                        .cast(field_type))

def calculate_arrival_datetime(df: DataFrame, state: dict[str, Any], field_type: str):
    # First, determine the format of report_time_utc
    df = df.withColumn('time_format',
                       F.when(F.length(F.col('report_time_utc')) == 5, 'HH:mm')
                        .otherwise('HH:mm:ss'))

    # Then, apply the appropriate timestamp conversion
    df = df.withColumn('arrival_datetime',
                       F.when(F.col('time_format') == 'HH:mm',
                              F.to_timestamp(
                                  F.concat(F.col('report_date_utc'), F.lit(' '), F.col('report_time_utc')),
                                  'yyyy-MM-dd HH:mm'
                              )
                       ).otherwise(
                           F.to_timestamp(
                               F.concat(F.col('report_date_utc'), F.lit(' '), F.col('report_time_utc')),
                               'yyyy-MM-dd HH:mm:ss'
                           )
                       ).cast(field_type))

    # Convert to UTC
    df = df.withColumn('arrival_datetime', 
                       F.from_utc_timestamp(F.col('arrival_datetime'), 'UTC'))

    # Drop the temporary 'time_format' column
    df = df.drop('time_format')

    return df


def calculate_ocurrence_datetime(df: DataFrame, state: dict[str, Any], field_type: str): 
    # Subtract the voyage_time_hs (in hours) from arrival_datetime
    df = df.withColumn('ocurrence_datetime', 
                       F.col('arrival_datetime') - F.make_dt_interval(hours = df.voyage_time_hs))
    
    return df

def calculate_me_hours(df: DataFrame, state: dict[str, Any], field_type: str):
    return df.withColumn('me_hours', 
                       (F.coalesce(F.col('me1_running_hours'), F.lit(0)) + 
                        F.coalesce(F.col('me2_running_hours'), F.lit(0))).cast(field_type))

def calculate_me_load(df: DataFrame, state: dict[str, Any], field_type: str):
    return df.withColumn('me_load', 
                       (F.coalesce(F.col('me1_load'), F.lit(0)) + 
                        F.coalesce(F.col('me2_load'), F.lit(0))).cast(field_type))

def calculate_me_power(df: DataFrame, state: dict[str, Any], field_type: str):
    return df.withColumn('me_power', 
                       (F.coalesce(F.col('me1_electric_power_kw'), F.lit(0)) + 
                        F.coalesce(F.col('me2_electric_power_kw'), F.lit(0))).cast(field_type))

def calculate_constant_emo_delta_fuel(df: DataFrame, state: dict[str, Any], field_type: str) -> DataFrame:
    # Create a DataFrame from the state dictionary
    state_df = df.sparkSession.createDataFrame([(k, v) for k, v in state.items()], ["ship", "value"])
    
    # Convert 'imo' column to uppercase
    df = df.withColumn('imo_upper', F.upper(F.col('imo')))
    
    # Convert 'ship' column in state_df to uppercase
    state_df = state_df.withColumn('ship_upper', F.upper(F.col('ship')))
    
    # Broadcast the state DataFrame for better performance
    state_df_broadcast = F.broadcast(state_df)
    
    # Join the main DataFrame with the state DataFrame
    df = df.join(state_df_broadcast, df.imo_upper == state_df_broadcast.ship_upper, 'left') \
        .drop('imo_upper', 'ship_upper', 'ship') \
        .withColumn('constant_emo_delta_fuel', 
                    F.col('value').cast(field_type)) \
        .drop('value')
    
    return df

def calculate_constant_delta_fuel_consumption(df: DataFrame, state: dict[str, Any], field_type: str):
    # Create a DataFrame from the state dictionary
    state_df = df.sparkSession.createDataFrame([(k, v) for k, v in state.items()], ["ship", "value"])
    
    # Convert 'imo' column to uppercase
    df = df.withColumn('imo_upper', F.upper(F.col('imo')))
    
    # Convert 'ship' column in state_df to uppercase
    state_df = state_df.withColumn('ship_upper', F.upper(F.col('ship')))
    
    # Broadcast the state DataFrame for better performance
    state_df_broadcast = F.broadcast(state_df)
    
    # Join the main DataFrame with the state DataFrame
    df = df.join(state_df_broadcast, df.imo_upper == state_df_broadcast.ship_upper, 'left') \
        .drop('imo_upper', 'ship_upper', 'ship') \
        .withColumn('constant_delta_fuel_consumption', 
                    F.col('value').cast(field_type)) \
        .drop('value')
    
    return df

def calculate_constant_aeu_lower(df: DataFrame, state: dict[str, Any], field_type: str) -> DataFrame:
    # Create a DataFrame from the state dictionary
    state_df = df.sparkSession.createDataFrame([(k, v) for k, v in state.items()], ["ship", "value"])
    
    # Convert 'imo' column to uppercase
    df = df.withColumn('imo_upper', F.upper(F.col('imo')))
    
    # Convert 'ship' column in state_df to uppercase
    state_df = state_df.withColumn('ship_upper', F.upper(F.col('ship')))
    
    # Broadcast the state DataFrame for better performance
    state_df_broadcast = F.broadcast(state_df)
    
    # Join the main DataFrame with the state DataFrame
    df = df.join(state_df_broadcast, df.imo_upper == state_df_broadcast.ship_upper, 'left') \
        .drop('imo_upper', 'ship_upper', 'ship') \
        .withColumn('constant_aeu_lower', 
                    F.col('value').cast(field_type)) \
        .drop('value')
    
    return df

def calculate_vehicle_email_operator1(df: DataFrame, state: dict[str, Any], field_type: str) -> DataFrame:
    # Create a DataFrame from the state dictionary
    state_df = df.sparkSession.createDataFrame([(k, v) for k, v in state.items()], ["tug", "email"])
    
    # Convert 'imo' column to uppercase
    df = df.withColumn('imo_upper', F.upper(F.col('imo')))
    
    # Convert 'tug' column in state_df to uppercase
    state_df = state_df.withColumn('tug_upper', F.upper(F.col('tug')))
    
    # Broadcast the state DataFrame for better performance
    state_df_broadcast = F.broadcast(state_df)
    
    # Join the main DataFrame with the state DataFrame
    df = df.join(state_df_broadcast, df.imo_upper == state_df_broadcast.tug_upper, 'left') \
        .drop('imo_upper', 'tug_upper', 'tug') \
        .withColumn('vehicle_email_operator1', 
                    F.col('email').cast(field_type)) \
        .drop('email')
    
    return df

def calculate_vehicle_email_operator2(df: DataFrame, state: dict[str, Any], field_type: str):
    # Create a DataFrame from the state dictionary
    state_df = df.sparkSession.createDataFrame([(k, v) for k, v in state.items()], ["tug", "email"])
    
    # Convert 'imo' column to uppercase
    df = df.withColumn('imo_upper', F.upper(F.col('imo')))
    
    # Convert 'tug' column in state_df to uppercase
    state_df = state_df.withColumn('tug_upper', F.upper(F.col('tug')))
    
    # Broadcast the state DataFrame for better performance
    state_df_broadcast = F.broadcast(state_df)
    
    # Join the main DataFrame with the state DataFrame
    df = df.join(state_df_broadcast, df.imo_upper == state_df_broadcast.tug_upper, 'left') \
        .drop('imo_upper', 'tug_upper', 'tug') \
        .withColumn('vehicle_email_operator2', 
                    F.col('email').cast(field_type)) \
        .drop('email')
    
    return df