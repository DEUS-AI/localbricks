import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from deus_lib.utils.reason_constants import *
from src.tasks.clients.common_behaviours import _default_nan_values, _init_behaviour_columns, _set_qualified, _validate_required_fields, _set_secondary_reasons
from functools import reduce

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def energytotal(row):
    logger.info("energytotal")
    
    all_eng_max_load = [row['aux_engine1_max_load'], row['aux_engine2_max_load'], row['aux_engine3_max_load']]
    lst = sorted(all_eng_max_load)
    const = row['aux_engine1_mcr_const']
    log_duration = row['voyage_time_hs']
    epsilon = 1  # Additional hour for cooldown/changeover
    one_max_gen = lst[-1] * const * (log_duration + epsilon)
    two_max_gen = (lst[-1] + lst[-2]) * const * (log_duration + epsilon)
    three_max_gen = (lst[-1] + lst[-2] + lst[-3]) * const * (log_duration + epsilon)

    if row['genrunning'] == 1 and row['energydemand_kwh'] <= one_max_gen:
        z = 1
    elif row['genrunning'] == 2 and row['energydemand_kwh'] > one_max_gen and row['energydemand_kwh'] <= two_max_gen:
        z = 1
    elif row['genrunning'] == 3 and row['energydemand_kwh'] > two_max_gen and row['energydemand_kwh'] <= three_max_gen:
        z = 1
    else:
        z = 0
    return z
    
#_calculate__Efficient_Auxiliary_Engine_Use
def calculate_eaeu(bvr_name, data: DataFrame, filters, *args):
    logger.info("_calculate__Efficient_Auxiliary_Engine_Use")
    col_prefix = bvr_name.lower()
    data = _init_behaviour_columns(data, col_prefix)

    #data = _verify_filter_exclusions(data, bvr_name, filters)

    required_fields = [
        'operational_mode',
        'me_hours',
        'ae_sfoc',
        'ae_cons'
    ]
    
    data, has_missing_values = _validate_required_fields(data, bvr_name, required_fields)

    if has_missing_values:
        return data

    for col in ['ae_power1', 'ae_power2', 'ae_power3', 'ae_hours1', 'ae_hours2', 'ae_hours3', 
                'aux_engine1_mcr_const', 'aux_engine2_mcr_const', 'aux_engine3_mcr_const',
                'aux_engine1_max_load', 'aux_engine2_max_load', 'aux_engine3_max_load',
                'aeu_lower_threshold', 'voyage_time_hs', 'ae_sfoc']:
        data = data.fillna({col: 0})

    exempted_modes = []
    
    other_reasons = {}
    other_reasons[NOT_POSSIBLE__NOT_AT_SEA] = (data['operational_mode'].isin(exempted_modes))
    other_reasons[NOT_POSSIBLE__MANOEUVRE_TIME_OVER_LIMIT] = (data['manoeuvring_hours'] > 4)
    
    # Check for missing required values using PySpark functions
    other_reasons[FAULTY_DATA__MISSING_REQUIRED_VALUES] = F.greatest(*[F.col(field).isNull().cast("int") for field in required_fields]) > 0
    other_reasons[FAULTY_DATA__INVALID_SFOC] = ~((data['ae_sfoc'] > 100) & (data['ae_sfoc'] < 400))
    other_reasons[FAULTY_DATA__INVALID_RUNNING_HOURS] = (data['ae_hours1'] + data['ae_hours2'] + data['ae_hours3'] < 0)
    other_reasons[FAULTY_DATA__INVALID_AUX_ENGINES_CONSUMPTION] = (data['ae_cons'] < 0)
    
    data = _set_secondary_reasons(data, bvr_name, other_reasons)

    eta = 0.96
    num_aes = 3

    data = data.withColumn('energydemand_kwh', sum((F.col(f'ae_power{i}') * F.col(f'ae_hours{i}') / eta) for i in range(1, num_aes+1)))

    for i in range(1, num_aes+1):
        data = data.withColumn(f'ae{i}_mcr', F.col(f'aux_engine{i}_mcr_const') * F.col(f'aux_engine{i}_max_load'))

    data = data.withColumn('genrunning', sum((F.col(f'ae_hours{i}') > 0).cast('int') for i in range(1, num_aes+1)))

    energy_udf = F.udf(energytotal, T.IntegerType())
    
    data = data.withColumn('energytotal_flag', energy_udf(F.struct([F.col(x) for x in data.columns])))
    denom = sum((F.col(f'aux_engine{i}_mcr_const') * F.col(f'ae{i}_mcr') * F.col(f'ae_hours{i}')) for i in range(1, num_aes+1))
    data = data.withColumn('aeu', F.when((F.col('energytotal_flag') == 1) | (F.col('energydemand_kwh') / denom >= 1), 100)
                           .otherwise((F.col('energydemand_kwh') / denom) * 100))
    
    data = data.fillna({'aeu': 100})

    eaeu_savings = ((F.col('aeu') - F.col('aeu_lower_threshold')) / 100 * F.col('ae_sfoc') * F.col('energydemand_kwh')) / 1000
    succeeded = (F.col('aeu') >= F.col('aeu_lower_threshold')) & (F.col(f"{col_prefix}__reason") == '')
    data = data.withColumn(f"{col_prefix}__net_savings", eaeu_savings)
    data = data.withColumn(f"{col_prefix}__success", succeeded)
    data = data.withColumn(f"{col_prefix}__savings", F.when(succeeded, eaeu_savings).otherwise(0))
    data = data.withColumn(f"{col_prefix}__best_case_savings", F.col(f"{col_prefix}__savings"))

    data = data.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None))

    return data
    

#_calculate__Engine_Maintenance_Optimisation
def calculate_emo(bvr_name, data: DataFrame, filters, *args):
    logger.info("_calculate__Engine_Maintenance_Optimisation")
    col_prefix = bvr_name.lower()
    data = _init_behaviour_columns(data, col_prefix)

    required_fields = [
        'me_cons',
        'operational_mode',
        'me_hours',
        'speed_bosp',
        'wind_speed_bf',
        'me_sfoc',
        'me_power'
    ]
    
    data, has_missing_values = _validate_required_fields(data, bvr_name, required_fields)
    
    if has_missing_values:
        return data
    
    exempted_modes = ['InPortMoored']
    
    other_reasons = {}
    other_reasons[NOT_POSSIBLE__NOT_AT_SEA] = F.col('operational_mode').isin(exempted_modes)
    other_reasons[NOT_POSSIBLE__MANOEUVRE_TIME_OVER_LIMIT] = F.col('manoeuvring_hours') > 4
    other_reasons[FAULTY_DATA__MISSING_REQUIRED_VALUES] = F.greatest(*[F.col(field).isNull().cast("int") for field in required_fields]) > 0
    
    data = data.fillna({
        'me_cons': 0,
        'me_sfoc': 0,
        'me_power': 0,
        'me_hours': 0,
        'speed_bosp': 0,
        'wind_speed_bf': 0
    })
    
    data = data.withColumn('me_sfoc', F.round(F.col('me_sfoc'), 0))
    
    other_reasons[FAULTY_DATA__INVALID_FUEL_REPORT] = ~((F.col('me_sfoc') > 100) & (F.col('me_sfoc') < 300))
    other_reasons[FAULTY_DATA__INVALID_RUNNING_HOURS] = (F.col('me_hours') < 20) | (F.col('me_hours') > 30)
    other_reasons[NOT_POSSIBLE__SPEED_UNDER_MIN] = F.col('speed_bosp') < 7
    other_reasons[NOT_POSSIBLE__BAD_WEATHER] = F.col('wind_speed_bf') >= 5
    
    data = _set_secondary_reasons(data, bvr_name, other_reasons)
    
    data = data.withColumn('me_energy_kwh', F.col('me_power') * F.col('me_hours'))
    data = data.withColumn('emo_total_fuel_used', F.col('me_energy_kwh') * F.col('me_sfoc'))
    data = data.withColumn('emo_ideal_fuel_used', F.col('me_energy_kwh') * F.col('emo_threshold'))
    data = data.withColumn('emo_max_fuel_used', F.col('emo_ideal_fuel_used'))
    data = data.withColumn('delta_sfoc', F.col('me_sfoc') - F.col('emo_threshold'))
    
    delta_fuel_used = (F.col('emo_ideal_fuel_used') - F.col('emo_total_fuel_used')) / 1000
    succeeded = (delta_fuel_used.isNotNull()) & (F.col('emo_total_fuel_used') <= F.col('emo_ideal_fuel_used')) & (F.col(f"{col_prefix}__reason") == '')
    
    data = data.withColumn(f"{col_prefix}__net_savings", delta_fuel_used)
    data = data.withColumn(f"{col_prefix}__success", succeeded)
    data = data.withColumn(f"{col_prefix}__savings", F.when(succeeded, delta_fuel_used).otherwise(0))
    data = data.withColumn(f"{col_prefix}__best_case_savings", F.col(f"{col_prefix}__savings"))
    data = data.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None).cast("int"))
    
    return data


def calculate_et(bvr_name, data: DataFrame, filters, *args):
    logger.info("_calculate__EfficiencyTrim")
    
    col_prefix = bvr_name.lower()
    data = _init_behaviour_columns(data, bvr_name=bvr_name)
    # data = super()._verify_filter_exclusions(data, bvr_name, filters)
    required_fields = [
        'me_cons',
        'me_hours',
        'speed_bosp',
        'wind_speed_bf',
        'trim',
        'load'
    ]
    data, has_missing_values = _validate_required_fields(data, bvr_name, required_fields)
    if has_missing_values:
        return data

    other_reasons = {}
    exempted_modes = ['InPortMoored']
    
    data = data.fillna({'operational_mode': ''})

    other_reasons[NOT_POSSIBLE__NOT_AT_SEA] = F.col('operational_mode').isin(exempted_modes)
    other_reasons[NOT_POSSIBLE__MANOEUVRE_TIME_OVER_LIMIT] = F.col('manoeuvring_hours') > 4
    other_reasons[FAULTY_DATA__MISSING_REQUIRED_VALUES] = reduce(lambda a, b: a | b, [F.col(field).isNull() for field in required_fields])
    other_reasons[NOT_POSSIBLE__TRIM_NOT_ACHIEVABLE_WHEN_BALLAST] = F.col('load') == 'B'
    other_reasons[FAULTY_DATA__INVALID_FUEL_REPORT] = F.col('me_cons') <= 0
    other_reasons[FAULTY_DATA__INVALID_RUNNING_HOURS] = (F.col('me_hours') < 20) | (F.col('me_hours') > 30)
    other_reasons[NOT_POSSIBLE__SPEED_UNDER_MIN] = F.col('speed_bosp') < 7
    other_reasons[NOT_POSSIBLE__BAD_WEATHER] = F.col('wind_speed_bf') >= 5
    data = _set_secondary_reasons(data, bvr_name, other_reasons)

    fuel_saved_per_hour_even_keel = 6.3
    fuel_reduction_per_unit_trim = 12.6

    calculate_fuel_saving_udf = F.udf(lambda trim: fuel_saved_per_hour_even_keel - abs(trim) * fuel_reduction_per_unit_trim, T.FloatType())
    determine_success_udf = F.udf(lambda trim: trim <= 0.5 and trim >= -0.25, T.BooleanType())

    data = data.fillna({'trim': -2})

    data = data.withColumn('succeeded', determine_success_udf(F.col('trim')) & (F.col(f"{col_prefix}__reason") == ''))
    data = data.withColumn('savings_kg', calculate_fuel_saving_udf(F.col('trim')) * F.col('me_hours'))

    data = data.withColumn(f"{col_prefix}__net_savings", F.col('savings_kg'))
    data = data.withColumn(f"{col_prefix}__success", F.col('succeeded'))
    data = data.withColumn(f"{col_prefix}__savings", F.when(F.col('succeeded'), F.col('savings_kg')).otherwise(0))
    data = data.withColumn(f"{col_prefix}__best_case_savings", F.col(f"{col_prefix}__savings"))

    data = data.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None).cast(T.BooleanType()))

    return data