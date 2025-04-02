from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType
from src.tasks.clients.common_behaviours import _set_secondary_reasons, _validate_required_fields, _verify_filter_exclusions
import deus_lib.utils.trim_calculator as TrimCalculator
import deus_lib.utils.sfoc_calculator as SFOCCalculator
from deus_lib.utils.reason_constants import *
from datetime import datetime



use_behaviour_comparisons = False

def __init__(customer_code):
    customer_code = customer_code
    spark = SparkSession.builder.appName("STCalculator").getOrCreate()

def genrunning(df: DataFrame) -> DataFrame:
    return df.withColumn("genrunning", 
        (F.when(F.col("generator_1_running_hours") > 0, 1).otherwise(0) +
         F.when(F.col("generator_2_running_hours") > 0, 1).otherwise(0) +
         F.when(F.col("generator_3_running_hours") > 0, 1).otherwise(0) +
         F.when(F.col("generator_4_running_hours") > 0, 1).otherwise(0))
    )
    
def energytotal(df: DataFrame) -> DataFrame:
        return df.withColumn("energytotal",
            F.when(
                (F.col("genrunning") == 1) & 
                (F.col("edemand") <= F.greatest("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load") * 
                                      F.col("constant_eaeu_const") * (24 + 1)),
                1
            ).when(
                (F.col("genrunning") == 2) & 
                (F.col("edemand") > F.greatest("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load") * 
                                     F.col("constant_eaeu_const") * (24 + 1)) &
                (F.col("edemand") <= (F.greatest("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load") + 
                                      F.array_sort(F.array("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load"))[2]) * 
                                      F.col("constant_eaeu_const") * (24 + 1)),
                1
            ).when(
                (F.col("genrunning") == 3) & 
                (F.col("edemand") > (F.greatest("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load") + 
                                     F.array_sort(F.array("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load"))[2]) * 
                                     F.col("constant_eaeu_const") * (24 + 1)) &
                (F.col("edemand") <= (F.greatest("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load") + 
                                      F.array_sort(F.array("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load"))[2] +
                                      F.array_sort(F.array("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load"))[1]) * 
                                      F.col("constant_eaeu_const") * (24 + 1)),
                1
            ).when(
                (F.col("genrunning") == 4) & 
                (F.col("edemand") > (F.greatest("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load") + 
                                     F.array_sort(F.array("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load"))[2] +
                                     F.array_sort(F.array("aux_engine1_max_load", "aux_engine2_max_load", "aux_engine3_max_load", "aux_engine4_max_load"))[1]) * 
                                     F.col("constant_eaeu_const") * (24 + 1)) &
                (F.col("edemand") <= (F.col("aux_engine1_max_load") + F.col("aux_engine2_max_load") + F.col("aux_engine3_max_load") + F.col("aux_engine4_max_load")) * 
                                      F.col("constant_eaeu_const") * (24 + 1)),
                1
            ).otherwise(0)
        )
        
@staticmethod
def change_format(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(
        f"{column_name}_formatted",
        F.date_format(F.to_date(F.col(column_name), "yyyyMMdd"), "dd/MM/yyyy")
    )
    
def calculate__Efficient_Auxiliary_Engine_Use(self, bvr_name: str, data: DataFrame, filters: list, *args) -> DataFrame:
        col_prefix = bvr_name.lower()
        data = _verify_filter_exclusions(data, bvr_name, filters)
        required_fields = [
            'generator_energy_hourly_avg_1',
            'generator_energy_hourly_avg_2',
            'generator_energy_hourly_avg_3',
            'generator_energy_hourly_avg_4',
            'generator_1_running_hours',
            'generator_2_running_hours',
            'generator_3_running_hours',
            'generator_4_running_hours',
            'speedthroughwatermehrs',
            'observeddistance',
            'constant_eaeu_eta',
            'constant_eaeu_const',
            'aux_engine1_max_load',
            'aux_engine1_min_load',
            'aux_engine2_max_load',
            'aux_engine2_min_load',
            'aux_engine3_max_load',
            'aux_engine3_min_load',
            'aux_engine4_max_load',
            'aux_engine4_min_load',
            'aeu_lower_threshold',
        ]
        data, has_missing_values = _validate_required_fields(data, bvr_name, required_fields)
        if has_missing_values:
            return data

        # Fill NaN values with 0 for specific columns
        for field in ['generator_energy_hourly_avg_1', 'generator_energy_hourly_avg_2', 'generator_energy_hourly_avg_3', 'generator_energy_hourly_avg_4',
                      'generator_1_running_hours', 'generator_2_running_hours', 'generator_3_running_hours', 'generator_4_running_hours',
                      'speedthroughwatermehrs', 'observeddistance', 'aux_engines_sfoc']:
            data = data.withColumn(field, F.coalesce(F.col(field).cast(FloatType()), F.lit(0.0)))

        # Calculate MCR for each auxiliary engine
        for i in range(1, 5):
            data = data.withColumn(f"ae{i}_mcr", F.col(f"aux_engine{i}_max_load") * F.col("constant_eaeu_const"))

        # Calculate the actual Load % of each auxiliary engine
        for i in range(1, 5):
            data = data.withColumn(f"ae{i}_load", F.col(f"generator_energy_hourly_avg_{i}") / F.col(f"aux_engine{i}_max_load"))

        # Calculate the actual energy demand
        data = data.withColumn("edemand", 
            ((F.col("ae1_mcr") * F.col("ae1_load") * F.col("generator_1_running_hours")) +
             (F.col("ae2_mcr") * F.col("ae2_load") * F.col("generator_2_running_hours")) +
             (F.col("ae3_mcr") * F.col("ae3_load") * F.col("generator_3_running_hours")) +
             (F.col("ae4_mcr") * F.col("ae4_load") * F.col("generator_4_running_hours"))) / F.col("constant_eaeu_eta")
        )

        # Apply genrunning and energytotal
        data = genrunning(data)
        data = energytotal(data)

        # Calculate the denominator for the AEU equation
        data = data.withColumn("aeu_denom",
            (F.col("constant_eaeu_const") * F.col("ae1_mcr") * F.col("generator_1_running_hours")) +
            (F.col("constant_eaeu_const") * F.col("ae2_mcr") * F.col("generator_2_running_hours")) +
            (F.col("constant_eaeu_const") * F.col("ae3_mcr") * F.col("generator_3_running_hours")) +
            (F.col("constant_eaeu_const") * F.col("ae4_mcr") * F.col("generator_4_running_hours"))
        )

        # Calculate AEU
        data = data.withColumn("aeu",
            F.when((F.col("energytotal") == 1) | (F.col("edemand") / F.col("aeu_denom") >= 1), 1)
             .otherwise(F.col("edemand") / F.col("aeu_denom"))
        )

        # Determine if EAEU was successful
        data = data.withColumn(f"{col_prefix}__success",
            (F.col("aeu") >= F.col("aeu_lower_threshold")) & (F.col(f"{col_prefix}__reason") == "")
        )

        # Calculate consumption for each generator
        for i in range(1, 5):
            data = data.withColumn(f"consumption_gen_{i}",
                F.col("aux_engines_sfoc") * F.col(f"generator_energy_hourly_avg_{i}") * F.col(f"generator_{i}_running_hours")
            )

        # Calculate total consumption
        data = data.withColumn("consumption",
            (F.col("consumption_gen_1") + F.col("consumption_gen_2") + 
             F.col("consumption_gen_3") + F.col("consumption_gen_4")) / 1000
        )

        # Calculate savings and other metrics
        data = data.withColumn(f"{col_prefix}__net_savings", (F.col("aeu") - F.col("aeu_lower_threshold")) * F.col("consumption"))
        data = data.withColumn(f"{col_prefix}__savings", 
            F.when(F.col(f"{col_prefix}__success"), F.col(f"{col_prefix}__net_savings")).otherwise(0)
        )
        data = data.withColumn(f"{col_prefix}__best_case_savings", 
            F.when(F.col(f"{col_prefix}__success"), (1 - F.col("aeu_lower_threshold")) * F.col("consumption")).otherwise(0)
        )

        # Set succeeded_predicted to None (null in Spark)
        data = data.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None))

        return data

def calculate__Optimal_Trim(bvr_name: str, data: DataFrame, filters: list, *args) -> DataFrame:
    col_prefix = bvr_name.lower()
    data = _verify_filter_exclusions(data, bvr_name, filters)
    required_fields = [
        'imo',
        'speedthroughwatermehrs',
        'observeddistance',
        'trim',
        'meandraft'
    ]
    data, has_missing_values = _validate_required_fields(data, bvr_name, required_fields)
    if has_missing_values:
        return data
    # Set secondary reasons
    data = _set_secondary_reasons(data, bvr_name, {
        NOT_POSSIBLE__TRIM_NOT_AVAILABLE: ~F.col('imo').cast(StringType()).isin(TrimCalculator.accepted_imos),
        FAULTY_DATA__MISSING_REQUIRED_VALUES: F.lit(False)  # This is handled by _validate_required_fields
    })
    # Fill NaN values with 0 for specific columns and cast to float
    for field in ['speedthroughwatermehrs', 'observeddistance', 'meandraft', 
                  'tank_cleaning_consumption_ifo', 'tank_cleaning_consumption_mgo', 
                  'tank_cleaning_consumption_lsf', 'tank_cleaning_consumption_mdo', 'beaufort']:
        data = data.withColumn(field, F.coalesce(F.col(field).cast(FloatType()), F.lit(0.0)))
    # Set more secondary reasons
    data = _set_secondary_reasons(data, bvr_name, {
        NOT_POSSIBLE__NOT_AT_SEA: (F.col('speedthroughwatermehrs') <= 0) | (F.col('observeddistance') <= 0),
        NOT_POSSIBLE__SPEED_UNDER_MIN: F.col('speedthroughwatermehrs') < 11,
        FAULTY_DATA__INVALID_SPEED: F.col('speedthroughwatermehrs') > 20,
        FAULTY_DATA__INVALID_MEAN_DRAFT: F.col('meandraft') > 20,
        NOT_POSSIBLE__TANK_CLEANING: (F.col('tank_cleaning_consumption_ifo') > 0) | 
                                             (F.col('tank_cleaning_consumption_mgo') > 0) | 
                                             (F.col('tank_cleaning_consumption_lsf') > 0) | 
                                             (F.col('tank_cleaning_consumption_mdo') > 0),
        NOT_POSSIBLE__BAD_WEATHER: F.col('beaufort') >= 6
    })
    # Limit speed to 15 for trim calculation
    data = data.withColumn('speedthroughwatermehrslimitedtrim', 
                           F.when(F.col('speedthroughwatermehrs') > 15, 15)
                            .otherwise(F.col('speedthroughwatermehrs')))
    # Create a UDF for trim calculations
    @F.udf(returnType=FloatType())
    def get_trim_percentage_udf(imo, draft, speed, trim):
        return TrimCalculator.get_trim_percentage(imo, draft, speed, trim, s3_controller)
    @F.udf(returnType=FloatType())
    def get_trim_limit_udf(imo):
        return TrimCalculator.get_trim_limit(imo)
    @F.udf(returnType=FloatType())
    def get_ideal_trim_udf(imo, draft, speed):
        return TrimCalculator.get_ideal_trim(imo, draft, speed, s3_controller)
    # Apply trim calculations
    data = data.withColumn('trim_perc_actual', 
                           get_trim_percentage_udf(F.col('imo'), F.col('meandraft'), 
                                                   F.col('speedthroughwatermehrslimitedtrim'), F.col('trim')))
    data = data.withColumn('trim_perc_limit', get_trim_limit_udf(F.col('imo')))
    data = data.withColumn('trim_ideal', 
                           get_ideal_trim_udf(F.col('imo'), F.col('meandraft'), 
                                              F.col('speedthroughwatermehrslimitedtrim')))
    # Set secondary reason for trim out of limits
    data = _set_secondary_reasons(data, bvr_name, {
        NOT_POSSIBLE__TRIM_OUT_OF_LIMITS: F.col('trim_perc_actual').isNull()
    })
    # Determine if trim was successful
    data = data.withColumn(f"{col_prefix}__success",
                           (F.col('trim_perc_actual').isNotNull()) & 
                           (F.col('trim_perc_actual') <= F.col('trim_perc_limit')) & 
                           (F.col(f"{col_prefix}__reason") == ""))
    # Calculate fuel consumption and savings
    data = data.withColumn('fuel_consumption', F.col('total_propulsion_consumption_new') * 1000)
    data = data.withColumn('fuel_alpha_best_case', F.lit(1.0))
    data = data.withColumn('fuel_alpha', 1 + F.col('trim_perc_actual') / 100)
    data = data.withColumn('fuel_beta', 1 + F.col('trim_perc_limit') / 100)
    data = data.withColumn('savings_kg', 
                           F.when(F.col('fuel_alpha') > 0, 
                                  F.col('fuel_consumption') * (F.col('fuel_beta') / F.col('fuel_alpha')) - F.col('fuel_consumption'))
                            .otherwise(0))
    data = data.withColumn('savings_kg_best_case', 
                           F.col('fuel_consumption') * (F.col('fuel_beta') / F.col('fuel_alpha_best_case')) - F.col('fuel_consumption'))
    # Set final columns
    data = data.withColumn('fuel_consumption_perfect_trim', F.col('fuel_consumption') * (1 / F.col('fuel_alpha')))
    data = data.withColumn('fuel_consumption_actual_trim', F.col('fuel_consumption'))
    data = data.withColumn(f"{col_prefix}__net_savings", F.col('savings_kg'))
    data = data.withColumn(f"{col_prefix}__savings", 
                           F.when(F.col(f"{col_prefix}__success"), F.col('savings_kg')).otherwise(0))
    data = data.withColumn(f"{col_prefix}__best_case_savings", 
                           F.when(F.col(f"{col_prefix}__success"), F.col('savings_kg_best_case')).otherwise(0))
    data = data.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None))
    
    return data



def calculate__Efficient_Power_Management(bvr_name: str, data: DataFrame, filters: list, *args) -> DataFrame:
        col_prefix = bvr_name.lower()
        data = _verify_filter_exclusions(data, bvr_name, filters)
        required_fields = [
            'epm_success',
            'epm_fuel_saved',
            'epm_best_fuel_saved',
            'total_hours',
            'overproduced_hours'
        ]
        data, has_missing_values = _validate_required_fields(data, bvr_name, required_fields)
        if has_missing_values:
            return data

        # Set secondary reasons
        data = _set_secondary_reasons(data, bvr_name, {
            FAULTY_DATA__MISSING_REQUIRED_VALUES: F.lit(False),  # This is handled by _validate_required_fields
            NOT_POSSIBLE__VOYAGE_TOO_SHORT: F.col('total_hours') <= 3,
            FAULTY_DATA__INVALID_RUNNING_HOURS: F.col('total_hours') > 30
        })

        # EPM was calculated with continuous data in the pre-processor lambda previously
        data = data.withColumn('success_preprocessed',
            (F.col('epm_success').isNotNull()) & 
            ((F.col('epm_success') == True) | (F.col('epm_success') == 1))
        )

        data = data.withColumn(f"{col_prefix}__success",
            (F.col('success_preprocessed') == True) & (F.col(f"{col_prefix}__reason") == "")
        )

        # Set final columns
        data = data.withColumn(f"{col_prefix}__net_savings", F.col('epm_fuel_saved'))
        data = data.withColumn(f"{col_prefix}__savings", 
            F.when(F.col(f"{col_prefix}__success"), F.col('epm_fuel_saved')).otherwise(0)
        )
        data = data.withColumn(f"{col_prefix}__best_case_savings", 
            F.when(F.col(f"{col_prefix}__success"), F.col('epm_best_fuel_saved')).otherwise(0)
        )
        data = data.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None))

        return data

    # ... (remaining methods to be translated)


def calculate__Engine_Maintenance_Optimisation(bvr_name: str, data: DataFrame, filters: list, *args) -> DataFrame:
        col_prefix = bvr_name.lower()
        data = _verify_filter_exclusions(data, bvr_name, filters)
        required_fields = [
            'imo',
            'steaming_hours',
            'mainenginerunninghrs',
            'main_engine_kwhrs',
            'emo_delta_fuel',
            'speedthroughwatermehrs',
            'observeddistance'
        ]
        data, has_missing_values = _validate_required_fields(data, bvr_name, required_fields)
        if has_missing_values:
            return data

        # Fill NaN values with 0 for specific columns and cast to float
        float_columns = ['steaming_hours', 'speedthroughwatermehrs', 'observeddistance', 'mainenginerunninghrs', 
                         'main_engine_kwhrs', 'emo_sfoc', 'generator_energy_hourly_avg_1', 'generator_energy_hourly_avg_2', 
                         'generator_energy_hourly_avg_3', 'generator_energy_hourly_avg_4']
        
        for col in float_columns:
            data = data.withColumn(col, F.coalesce(F.col(col).cast(FloatType()), F.lit(0.0)))

        data = data.withColumn('imo', F.col('imo').cast(StringType()))

        # Create UDF for SFOC calculations
        @F.udf(returnType=FloatType())
        def get_sfoc_ideals_udf(imo):
            return SFOCCalculator.get_sfoc_ideals(imo, s3_controller)['sfoc_st']

        @F.udf(returnType=FloatType())
        def get_sfoc_min_udf(imo):
            return SFOCCalculator.get_sfoc_ideals(imo, s3_controller)['sfoc_min_st']

        @F.udf(returnType=FloatType())
        def get_sfoc_max_udf(imo):
            return SFOCCalculator.get_sfoc_ideals(imo, s3_controller)['sfoc_max_st']

        # Apply SFOC calculations
        data = data.withColumn('sfoc', get_sfoc_ideals_udf(F.col('imo')))
        data = data.withColumn('sfoc_min', get_sfoc_min_udf(F.col('imo')))
        data = data.withColumn('sfoc_max', get_sfoc_max_udf(F.col('imo')))

        # Set secondary reasons
        data = _set_secondary_reasons(data, bvr_name, {
            FAULTY_DATA__MISSING_REQUIRED_VALUES: F.lit(False),  # This is handled by _validate_required_fields
            NOT_POSSIBLE__NOT_AT_SEA: (F.col('speedthroughwatermehrs') <= 0) | (F.col('observeddistance') <= 0),
            NOT_POSSIBLE__VOYAGE_TOO_SHORT: F.col('steaming_hours') < 6,
            FAULTY_DATA__INVALID_RUNNING_HOURS: (F.col('mainenginerunninghrs') < 20) | (F.col('mainenginerunninghrs') > 30),
            FAULTY_DATA__INVALID_SFOC: (F.col('emo_sfoc').isNull()) | (F.col('emo_sfoc') < 50) | (F.col('emo_sfoc') > 400),
            FAULTY_DATA__INVALID_KWHRS: (F.col('main_engine_kwhrs') > 10000) | 
                                                (F.col('generator_energy_hourly_avg_1') > 7500) | 
                                                (F.col('generator_energy_hourly_avg_2') > 7500) | 
                                                (F.col('generator_energy_hourly_avg_3') > 7500) | 
                                                (F.col('generator_energy_hourly_avg_4') > 7500),
            FAULTY_DATA__INVALID_PROPULSION_CONSUMPTION: (F.col('total_propulsion_consumption_new') <= 0) | 
                                                                (F.col('total_propulsion_consumption_new') > 100),
            FAULTY_DATA__INVALID_GENERATOR_CONSUMPTION: (F.col('total_generator_consumption_new') <= 0) | 
                                                                (F.col('total_generator_consumption_new') > 100),
            FAULTY_DATA__SFOC_NOT_AVAILABLE: F.col('sfoc_min').isNull()
        })

        # Calculate main engine power and total fuel used
        data = data.withColumn('main_engine_power', F.col('mainenginerunninghrs') * F.col('main_engine_kwhrs'))
        data = data.withColumn('total_fuel_used', F.col('emo_sfoc') * F.col('main_engine_power') / 1000)
        data = data.withColumn('emo_total_fuel_used', F.col('total_fuel_used'))

        # Calculate delta SFOC
        data = data.withColumn('delta_sfoc', (F.col('emo_sfoc') - F.col('sfoc_min')) / F.col('sfoc_min') * 100)

        # Set secondary reason for invalid delta SFOC
        data = _set_secondary_reasons(data, bvr_name, {
            FAULTY_DATA__INVALID_DELTA_SFOC: F.col('delta_sfoc') > 100
        })

        # Calculate ideal fuel used and max/min fuel used
        data = data.withColumn('emo_ideal_fuel_used', F.col('total_fuel_used') / (1 + F.col('delta_sfoc') / 100))
        data = data.withColumn('emo_max_fuel_used', F.col('emo_ideal_fuel_used') * (1 + F.col('emo_delta_fuel') / 100))
        data = data.withColumn('emo_min_fuel_used', F.col('emo_ideal_fuel_used') * (1 - F.col('emo_delta_fuel') / 100))

        # Calculate delta fuel used
        data = data.withColumn('emo_delta_fuel_used', F.col('emo_max_fuel_used') - F.col('total_fuel_used'))
        data = data.withColumn('emo_min_fuel_used', 
            F.when(F.col('total_fuel_used') < F.col('emo_min_fuel_used'), F.col('total_fuel_used'))
             .otherwise(F.col('emo_min_fuel_used')))

        # Determine if EMO was successful
        data = data.withColumn(f"{col_prefix}__success",
            (F.col('emo_delta_fuel_used') > 0) & 
            (F.col('delta_sfoc') < F.col('emo_delta_fuel')) & 
            (F.col(f"{col_prefix}__reason") == "")
        )

        # Set final columns
        data = data.withColumn(f"{col_prefix}__net_savings", F.col('emo_delta_fuel_used'))
        data = data.withColumn(f"{col_prefix}__savings", 
            F.when(F.col(f"{col_prefix}__success"), F.col('emo_delta_fuel_used')).otherwise(0)
        )
        data = data.withColumn(f"{col_prefix}__best_case_savings", 
            F.when(F.col(f"{col_prefix}__success"), F.col('emo_max_fuel_used') - F.col('emo_min_fuel_used')).otherwise(0)
        )
        data = data.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None))

        return data

    # ... (any remaining methods or class closure)