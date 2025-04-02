from pyspark.sql import functions as F
from pyspark.sql.types import *

from src.tasks.clients.common_merge import TYPE_MAP


def apply_cast(df, column, field_type):
    return df.withColumn(column, df[column].cast(TYPE_MAP[field_type]))

def calculate_arrival_datetime(df, state, field_type):
    df = df.withColumn('arrival_datetime', F.to_timestamp('report_date'))
    return apply_cast(df, 'arrival_datetime', field_type)

def calculate_ocurrence_datetime(df, state, field_type):
    df = df.withColumn('ocurrence_datetime', 
        F.expr("arrival_datetime - voyage_time_hs * INTERVAL '1' HOUR - manoeuvring_hours * INTERVAL '1' HOUR"))
    return apply_cast(df, 'ocurrence_datetime', field_type)

def calculate_vehicle(df, state, field_type):
    vehicle_udf = F.udf(lambda imo: next((k for k, v in state.items() if str(v) == str(imo)), None), StringType())
    df = df.withColumn('vehicle', vehicle_udf(F.col('imo')))
    return apply_cast(df, 'vehicle', field_type)

def calculate_origin(df, state, field_type):
    df = df.withColumn('origin', F.when(F.col('origin').isNull(), 'UNKNOWN').otherwise(F.col('origin')))
    return apply_cast(df, 'origin', field_type)

def calculate_destination(df, state, field_type):
    df = df.withColumn('destination', F.when(F.col('destination').isNull(), 'UNKNOWN').otherwise(F.col('destination')))
    return apply_cast(df, 'destination', field_type)

def calculate_aux_engine1_max_load(df, state, field_type):
    aux_engine1_max_load_udf = F.udf(lambda imo: state.get(str(imo), None), IntegerType())

    df = df.withColumn('aux_engine1_max_load', aux_engine1_max_load_udf(F.col('imo')))

    return apply_cast(df, 'aux_engine1_max_load', field_type)

def calculate_aux_engine2_max_load(df, state, field_type):
    aux_engine2_max_load_udf = F.udf(lambda imo: state.get(str(imo), None), IntegerType())
    df = df.withColumn('aux_engine2_max_load', aux_engine2_max_load_udf(F.col('imo')))
    return apply_cast(df, 'aux_engine2_max_load', field_type)

def calculate_aux_engine3_max_load(df, state, field_type):
    aux_engine3_max_load_udf = F.udf(lambda imo: state.get(str(imo), None), IntegerType())
    df = df.withColumn('aux_engine3_max_load', aux_engine3_max_load_udf(F.col('imo')))
    return apply_cast(df, 'aux_engine3_max_load', field_type)

def calculate_aux_engine1_mcr_const(df, state, field_type):
    aux_engine1_mcr_const_udf = F.udf(lambda imo: state.get(str(imo), None), FloatType())
    df = df.withColumn('aux_engine1_mcr_const', aux_engine1_mcr_const_udf(F.col('imo')))
    return apply_cast(df, 'aux_engine1_mcr_const', field_type)

def calculate_aux_engine2_mcr_const(df, state, field_type):
    aux_engine2_mcr_const_udf = F.udf(lambda imo: state.get(str(imo), None), FloatType())
    df = df.withColumn('aux_engine2_mcr_const', aux_engine2_mcr_const_udf(F.col('imo')))
    return apply_cast(df, 'aux_engine2_mcr_const', field_type)

def calculate_aux_engine3_mcr_const(df, state, field_type):
    aux_engine3_mcr_const_udf = F.udf(lambda imo: state.get(str(imo), None), FloatType())
    df = df.withColumn('aux_engine3_mcr_const', aux_engine3_mcr_const_udf(F.col('imo')))
    return apply_cast(df, 'aux_engine3_mcr_const', field_type)

def calculate_aeu_lower_threshold(df, state, field_type):
    aeu_lower_threshold_udf = F.udf(lambda imo: state.get(str(imo), None), IntegerType())
    df = df.withColumn('aeu_lower_threshold', aeu_lower_threshold_udf(F.col('imo')))
    return apply_cast(df, 'aeu_lower_threshold', field_type)

def calculate_emo_threshold(df, state, field_type):
    emo_threshold_udf = F.udf(lambda imo: state.get(str(imo), None), FloatType())
    df = df.withColumn('emo_threshold', emo_threshold_udf(F.col('imo')))
    return apply_cast(df, 'emo_threshold', field_type)

def calculate_team_name(df, state, field_type):
    team_name_udf = F.udf(lambda imo: state.get(str(imo), None), StringType())
    df = df.withColumn('team_name', team_name_udf(F.col('imo')))
    return apply_cast(df, 'team_name', field_type)

def calculate_vehicle_email_operator1(df, state, field_type):
    vehicle_email_operator1_udf = F.udf(lambda imo: state.get(str(imo), None), StringType())
    df = df.withColumn('vehicle_email_operator1', vehicle_email_operator1_udf(F.col('imo')))
    return apply_cast(df, 'vehicle_email_operator1', field_type)

def calculate_vehicle_email_operator2(df, state, field_type):
    vehicle_email_operator2_udf = F.udf(lambda imo: state.get(str(imo), None), StringType())
    df = df.withColumn('vehicle_email_operator2', vehicle_email_operator2_udf(F.col('imo')))
    return apply_cast(df, 'vehicle_email_operator2', field_type)
