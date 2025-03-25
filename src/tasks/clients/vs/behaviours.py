from typing import Any, Optional
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from deus_lib.utils.reason_constants import *
import deus_lib.utils.trim_calculator as TrimCalculator
from src.tasks.clients.common_behaviours import BaseCalculatorBehaviours

MAX_ALLOWED_TAXI_TIME = 180

class BehavioursCalculator(BaseCalculatorBehaviours):

    def __init__(self, df: DataFrame, customer_code: str):

        df = self.preprocess_df(df = df)
        super.__init__(df = df, customer_code = customer_code)


    @staticmethod
    def preprocess_df(df: DataFrame) -> DataFrame:

        df = df.filter(F.col('voyage_time_hs') > 0) # this should be moved into gx and well as if there is no data here, stop the pipeline
        if df.isEmpty():
            return ValueError(f"Dataframe has no data for column voyage_time_hs>0, stopping execution of the pipeline")

        return df

    def _calculate__Reduced_Engine_Taxi_In(self, bvr_name: str, df: DataFrame, filters, *args) -> DataFrame:
        col_prefix = bvr_name.lower()
        #df = _verify_filter_exclusions(df, bvr_name, filters)
        df = self._init_behaviour_columns(df, bvr_name=bvr_name)

        required_fields = [
            'plnd_taxi_fuel',
            'taxi_in_minutes',
            'fuel_burn_taxi_in1_kg',
            'fuel_burn_taxi_in2_kg',
            'fuel_burn_reti_main_engine',
            'act_taxi_in_fuel',
            'constant_reti_cooldown_time',
            'constant_reti_threshold'
        ]

        df, has_missing_values = self._validate_required_fields(df, bvr_name, required_fields)

        if has_missing_values:
            return df

        df = df.withColumn('taxi_in_minutes', F.col('taxi_in_minutes').cast('decimal(18,5)'))
        df = df.withColumn('act_taxi_in_fuel', F.col('act_taxi_in_fuel').cast('decimal(18,5)'))
        df = df.withColumn('plnd_taxi_fuel', F.col('plnd_taxi_fuel').cast('decimal(18,5)'))
        df = df.withColumn('constant_reti_cooldown_time', F.col('constant_reti_cooldown_time').cast('decimal(18,5)'))
        df = df.withColumn('constant_reti_threshold', F.col('constant_reti_threshold').cast('decimal(18,5)'))
        df = df.withColumn('fuel_burn_reti_main_engine', F.col('fuel_burn_reti_main_engine').cast('decimal(18,5)'))

        if 'fuel_burn_taxi_in3_kg' not in df.columns:
            df = df.withColumn('fuel_burn_taxi_in3_kg', F.lit(0).cast('decimal(18,5)'))
        if 'fuel_burn_taxi_in4_kg' not in df.columns:
            df = df.withColumn('fuel_burn_taxi_in4_kg', F.lit(0).cast('decimal(18,5)'))

        df = df.fillna({'fuel_burn_taxi_in1_kg': 0, 'fuel_burn_taxi_in2_kg': 0, 'fuel_burn_taxi_in3_kg': 0, 'fuel_burn_taxi_in4_kg': 0})

        engines_pairs = F.when(F.col('fuel_burn_taxi_in3_kg') + F.col('fuel_burn_taxi_in4_kg') > 0, 2).otherwise(1)
        taxi_engine_mean_consumption = F.col('act_taxi_in_fuel') / engines_pairs

        other_reasons = {}
        other_reasons[FAULTY_DATA__MISSING_REQUIRED_VALUES] = F.isnan(F.col('taxi_in_minutes')) | F.isnan(F.col('act_taxi_in_fuel'))
        other_reasons[NOT_POSSIBLE__COVID_PERIOD] = (F.col('ocurrence_datetime') >= '2020-03-17') & (F.col('ocurrence_datetime') <= '2021-09-05')
        other_reasons[NOT_POSSIBLE__4_ENGINE_VEHICLE] = F.col('vehicle').rlike('B74|A34')
        other_reasons[FAULTY_DATA__INVALID_TAXI_FUEL] = (taxi_engine_mean_consumption <= 0) | (F.col('act_taxi_in_fuel') <= 0)
        other_reasons[NOT_POSSIBLE__UNDER_MIN_COOLDOWN] = F.col('taxi_in_minutes') < F.col('constant_reti_cooldown_time')
        other_reasons[NOT_POSSIBLE__TAXI_OVER_LIMIT] = (F.col('taxi_in_minutes') > MAX_ALLOWED_TAXI_TIME) | ((F.col('planned_taxi_time') > 0) 
                                                                                                                  & (F.col('taxi_in_minutes') > F.col('planned_taxi_time') * 1.2))
        other_reasons[FAULTY_DATA__OUTLIER_VALUES] = (F.col('plnd_taxi_fuel') > 0) & ((F.col('act_taxi_in_fuel') < F.col('plnd_taxi_fuel') * 0.1) 
                                                                                    | (F.col('act_taxi_in_fuel') > F.col('plnd_taxi_fuel') * 1.2))

        df = self._set_secondary_reasons(df, bvr_name, other_reasons)

        taxi_in_fuel_per_minute1 = F.when(F.col('taxi_in_minutes') > 0, F.col('fuel_burn_taxi_in1_kg') / F.col('taxi_in_minutes')).otherwise(0)
        taxi_in_fuel_per_minute2 = F.when(F.col('taxi_in_minutes') > 0, F.col('fuel_burn_taxi_in2_kg') / F.col('taxi_in_minutes')).otherwise(0)
        taxi_in_fuel_per_minute3 = F.when(F.col('taxi_in_minutes') > 0, F.col('fuel_burn_taxi_in3_kg') / F.col('taxi_in_minutes')).otherwise(0)
        taxi_in_fuel_per_minute4 = F.when(F.col('taxi_in_minutes') > 0, F.col('fuel_burn_taxi_in4_kg') / F.col('taxi_in_minutes')).otherwise(0)

        taxi_in_fuel_per_minute_max = F.greatest(taxi_in_fuel_per_minute1, taxi_in_fuel_per_minute2, taxi_in_fuel_per_minute3, taxi_in_fuel_per_minute4)
        fuel_burned_during_cooldown_kg = taxi_in_fuel_per_minute_max * F.col('constant_reti_cooldown_time')

        engine_1_fuel_burn = F.when(F.col('fuel_burn_taxi_in1_kg') > fuel_burned_during_cooldown_kg, F.col('fuel_burn_taxi_in1_kg') - fuel_burned_during_cooldown_kg).otherwise(0)
        engine_2_fuel_burn = F.when(F.col('fuel_burn_taxi_in2_kg') > fuel_burned_during_cooldown_kg, F.col('fuel_burn_taxi_in2_kg') - fuel_burned_during_cooldown_kg).otherwise(0)
        engine_3_fuel_burn = F.when(F.col('fuel_burn_taxi_in3_kg') > fuel_burned_during_cooldown_kg, F.col('fuel_burn_taxi_in3_kg') - fuel_burned_during_cooldown_kg).otherwise(0)
        engine_4_fuel_burn = F.when(F.col('fuel_burn_taxi_in4_kg') > fuel_burned_during_cooldown_kg, F.col('fuel_burn_taxi_in4_kg') - fuel_burned_during_cooldown_kg).otherwise(0)

        df = df.withColumn("fuel_burn_reti_1_without_cooldown", engine_1_fuel_burn.cast('decimal(18,5)'))
        df = df.withColumn("fuel_burn_reti_2_without_cooldown", engine_2_fuel_burn.cast('decimal(18,5)'))
        df = df.withColumn("fuel_burn_reti_3_without_cooldown", engine_3_fuel_burn.cast('decimal(18,5)'))
        df = df.withColumn("fuel_burn_reti_4_without_cooldown", engine_4_fuel_burn.cast('decimal(18,5)'))

        df = df.withColumn('fuel_burn_reti_main_engine', F.greatest(engine_1_fuel_burn, engine_2_fuel_burn, engine_3_fuel_burn, engine_4_fuel_burn).cast('decimal(18,5)'))

        main_engine_threshold = F.col('fuel_burn_reti_main_engine') * (1 - F.col('constant_reti_threshold'))

        reti_savings = F.when(engine_1_fuel_burn <= main_engine_threshold, F.col('fuel_burn_reti_main_engine') - engine_1_fuel_burn).otherwise(0)
        reti_savings += F.when(engine_2_fuel_burn <= main_engine_threshold, F.col('fuel_burn_reti_main_engine') - engine_2_fuel_burn).otherwise(0)
        reti_savings += F.when((engine_3_fuel_burn > 0) & (engine_3_fuel_burn <= main_engine_threshold), F.col('fuel_burn_reti_main_engine') - engine_3_fuel_burn).otherwise(0)
        reti_savings += F.when((engine_4_fuel_burn > 0) & (engine_4_fuel_burn <= main_engine_threshold), F.col('fuel_burn_reti_main_engine') - engine_4_fuel_burn).otherwise(0)

        # If an engine was reduced and fuel was saved, RETI was a success
        required_conditions = (main_engine_threshold > 0) & (reti_savings > 0) & (df[f"{col_prefix}__reason"] == '')
        df = df.withColumn(f"{col_prefix}__net_savings", reti_savings.cast('decimal(18,5)').alias(f"{col_prefix}__net_savings"))
        df = df.withColumn(f"{col_prefix}__success", F.when(required_conditions, True).otherwise(False))
        df = df.withColumn(f"{col_prefix}__savings", F.when(required_conditions, F.round(reti_savings.cast('decimal(18,5)'), 2)).otherwise(0))
        df = df.withColumn(f"{col_prefix}__best_case_savings", F.round((F.col('fuel_burn_reti_main_engine') * engines_pairs).cast('decimal(18,5)'), 2))

        return df

    def _calculate__Reduced_Engine_Taxi_Out(self, bvr_name: str, df: DataFrame, filters, *args) -> DataFrame:
            col_prefix = bvr_name

            # Verificación de exclusiones y validación de campos requeridos
            required_fields = [
                'plnd_taxi_fuel',
                'taxi_out_minutes',
                'fuel_burn_taxi_out1_kg',
                'fuel_burn_taxi_out2_kg',
                'act_taxi_out_fuel',
                'fuel_burn_reto_main_engine',
                'constant_reto_warmup_time',
                'constant_reto_threshold'
            ]

            #df = self._verify_filter_exclusions(df, col_prefix)
            df, has_missing_values = self._validate_required_fields(df, bvr_name, required_fields)

            if has_missing_values:
                return df

            df = df.fillna({
                'plnd_taxi_fuel': 0,
                'taxi_out_minutes': 0,
                'act_taxi_out_fuel': 0,
                'fuel_burn_taxi_out1_kg': 0,
                'fuel_burn_taxi_out2_kg': 0,
                'fuel_burn_taxi_out3_kg': 0,
                'fuel_burn_taxi_out4_kg': 0
            })

            df = df.withColumn('engines_pairs', 
                F.when(F.col('fuel_burn_taxi_out3_kg') + F.col('fuel_burn_taxi_out4_kg') > 0, 2).otherwise(1).cast('decimal(18,5)')
            )

            df = df.withColumn('taxi_engine_mean_consumption', 
                (F.col('act_taxi_out_fuel') / F.col('engines_pairs')).cast('decimal(18,5)')
            )

            df = df.withColumn('reto__other_reasons', F.struct(
                (F.col('ocurrence_datetime') >= '2020-03-17') & (F.col('ocurrence_datetime') <= '2021-09-05').alias('NOT_POSSIBLE__COVID_PERIOD'),
                F.col('vehicle').rlike('B74|A34').alias('NOT_POSSIBLE__4_ENGINE_VEHICLE'),
                (F.col('taxi_engine_mean_consumption') <= 0) | (F.col('act_taxi_out_fuel') <= 0).alias('FAULTY_DATA__INVALID_TAXI_FUEL'),
                (F.col('taxi_out_minutes') < F.col('constant_reto_warmup_time')).alias('NOT_POSSIBLE__UNDER_MIN_WARMUP'),
                (F.col('taxi_out_minutes') > MAX_ALLOWED_TAXI_TIME) | 
                ((F.col('planned_taxi_time') > 0) & (F.col('taxi_out_minutes') > F.col('planned_taxi_time') * 1.2)).alias('NOT_POSSIBLE__TAXI_OVER_LIMIT'),
                (F.col('plnd_taxi_fuel') > 0) & 
                ((F.col('act_taxi_out_fuel') < F.col('plnd_taxi_fuel') * 0.1) | (F.col('act_taxi_out_fuel') > F.col('plnd_taxi_fuel') * 1.2)).alias('FAULTY_DATA__OUTLIER_VALUES')
            ))

            df = self._set_secondary_reasons(df, col_prefix)

            taxi_out_fuel_per_minute_cols = ['fuel_burn_taxi_out1_kg', 'fuel_burn_taxi_out2_kg', 'fuel_burn_taxi_out3_kg', 'fuel_burn_taxi_out4_kg']
            for col in taxi_out_fuel_per_minute_cols:
                df = df.withColumn(f"{col}_per_minute", 
                    (F.when(F.col('taxi_out_minutes') > 0, F.col(col) / F.col('taxi_out_minutes'))).otherwise(0).cast('decimal(18,5)')
                )

            df = df.withColumn('taxi_out_fuel_per_minute_max', 
                (F.greatest(*[F.col(f"{col}_per_minute") for col in taxi_out_fuel_per_minute_cols])).cast('decimal(18,5)')
            )

            df = df.withColumn('fuel_burned_during_warmup_kg', 
                (F.col('taxi_out_fuel_per_minute_max') * F.col('constant_reto_warmup_time')).cast('decimal(18,5)')
            )

            for i in range(1, 5):
                df = df.withColumn(f"fuel_burn_reto_{i}_without_warmup", 
                    F.when(F.col(f"fuel_burn_taxi_out{i}_kg") > F.col('fuel_burned_during_warmup_kg'), 
                        (F.col(f"fuel_burn_taxi_out{i}_kg") - F.col('fuel_burned_during_warmup_kg'))).otherwise(0).cast('decimal(18,5)')
                )

            df = df.withColumn('fuel_burn_reto_main_engine', 
                F.greatest(*[F.col(f"fuel_burn_reto_{i}_without_warmup") for i in range(1, 5)]).cast('decimal(18,5)')
            )

            df = df.withColumn('main_engine_threshold', 
                (F.col('fuel_burn_reto_main_engine') * (1 - F.col('constant_reto_threshold'))).cast('decimal(18,5)')
            )

            net_reto_savings = F.col('fuel_burn_reto_main_engine') * 2 - F.col('fuel_burn_reto_1_without_warmup') - F.col('fuel_burn_reto_2_without_warmup')

            df = df.withColumn(f"{col_prefix}__net_savings", net_reto_savings.cast('decimal(18,5)'))
            df = df.withColumn(f"{col_prefix}__success", 
                (F.col('main_engine_threshold') > 0) & 
                (net_reto_savings > 0) & 
                (F.col(f"{col_prefix}__reason") == '')
            )
            df = df.withColumn(f"{col_prefix}__savings", 
                F.when(F.col(f"{col_prefix}__success"), net_reto_savings).otherwise(0)
            )
            df = df.withColumn(f"{col_prefix}__best_case_savings", 
                (F.col('fuel_burn_reto_main_engine') * F.col('engines_pairs')).cast('decimal(18,5)')
            )

            return df
    
    def _calculate__Efficient_Flight(self, bvr_name: str, df: DataFrame, filters, *args) -> DataFrame:
        col_prefix = bvr_name.lower()
        df = self._init_behaviour_columns(df, bvr_name=bvr_name)

        # Verificación de exclusiones y validación de campos requeridos
        required_fields = [
            'origin',
            'destination',
            'planned_fuel_burn_adjustment',
            'plnd_trip_fuel',
            'plnd_takeoff_weight',
            'loadsheet_tow',
            'act_trip_fuel'
        ]

        df, has_missing_values = self._validate_required_fields(df, bvr_name, required_fields)

        if has_missing_values:
            return df

        df = df.fillna({
            'plnd_trip_fuel': 0,
            'plnd_takeoff_weight': 0,
            'loadsheet_tow': 0,
            'act_trip_fuel': 0,
            'plnd_taxi_fuel': 0,
            'planned_taxi_time': 0
        })

        if 'plnd_taxi_fuel' in df.columns:
            df = df.withColumn('act_taxi_fuel', 
                F.coalesce(F.col('act_taxi_out_fuel'), F.lit(0)) + F.coalesce(F.col('act_taxi_in_fuel'), F.lit(0)).cast('decimal(18,5)')
            )
            
            df = df.withColumn('act_taxi_minutes', 
                F.coalesce(F.col('taxi_in_minutes'), F.lit(0)) + F.coalesce(F.col('taxi_out_minutes'), F.lit(0)).cast('decimal(18,5)')
            )
            
            df = df.withColumn('actual_trip_fuel', 
                F.when(
                    (F.col('plnd_taxi_fuel') > 0) & 
                    ((F.col('act_taxi_fuel') < F.col('plnd_taxi_fuel') * 0.2) | 
                     (F.col('act_taxi_minutes') < F.col('planned_taxi_time') * 0.2)), 
                    F.col('act_trip_fuel') - F.col('plnd_taxi_fuel')
                ).otherwise(F.col('act_trip_fuel')).cast('decimal(18,5)')
            )
            
            df = df.withColumn('actual_trip_fuel', 
                (F.floor(F.col('actual_trip_fuel') / 100) * 100).cast('decimal(18,5)')
            )

        df = df.withColumn('weight_diff_plan_vs_actual', 
            (F.col('loadsheet_tow') - F.col('plnd_takeoff_weight')).cast('decimal(18,5)')
        )
        
        df = df.withColumn('projected_trip_fuel', 
            (F.col('plnd_trip_fuel') + 
            (F.col('weight_diff_plan_vs_actual') / 1000) * F.col('planned_fuel_burn_adjustment')).cast('decimal(18,5)')
        )
        
        df = df.withColumn('projected_trip_fuel', 
            (F.col('projected_trip_fuel') + 
            ((F.col('weight_diff_plan_vs_actual') / 1000) * F.col('planned_fuel_burn_adjustment') / 1000) * F.col('planned_fuel_burn_adjustment')).cast('decimal(18,5)')
        )
        
        df = df.withColumn('projected_trip_fuel', 
            (F.ceil(F.col('projected_trip_fuel') / 100) * 100 + 50).cast('decimal(18,5)')
        )
        
        df = df.withColumn('ef_savings', 
            (F.col('projected_trip_fuel') - F.col('actual_trip_fuel')).cast('decimal(18,5)')
        )

        df = df.withColumn('other_reasons', F.struct(
            (F.col('ocurrence_datetime') >= '2020-03-17') & (F.col('ocurrence_datetime') <= '2021-09-05').alias('NOT_POSSIBLE__COVID_PERIOD'),
            F.col('vehicle').rlike('B74|A34').alias('NOT_POSSIBLE__4_ENGINE_VEHICLE'),
            (F.col('loadsheet_tow') <= 0).alias('FAULTY_DATA__INVALID_TOW'),
            (F.col('origin') == F.col('destination')).alias('NOT_POSSIBLE__INVALID_LEG'),
            ((F.col('actual_trip_fuel') < F.col('projected_trip_fuel') * 0.8) | 
             (F.col('actual_trip_fuel') > F.col('projected_trip_fuel') * 1.1)).alias('FAULTY_DATA__OUTLIER_VALUES')
        ))

        if 'plnd_flight_time_hs' in df.columns or 'plnd_airborne_time' in df.columns:
            df = df.fillna({'plnd_airborne_time': 0, 'airborne_time': 0})
            
            df = df.withColumn('plnd_flight_time_mins', 
                F.when(
                    (F.col('plnd_flight_time_hs').isNotNull()), 
                    F.col('plnd_flight_time_hs') * 60
                ).otherwise(F.col('plnd_airborne_time') * 60).cast('decimal(18,5)')
            )
            
            df = df.withColumn('act_flight_time_mins', 
                F.when(
                    (F.col('actual_flight_time_hs').isNotNull()), 
                    F.col('actual_flight_time_hs') * 60
                ).otherwise(F.col('airborne_time') * 60).cast('decimal(18,5)')
            )
            
            df = df.withColumn('max_delay_allowed', 
                (F.col('plnd_flight_time_mins') + 20).cast('decimal(18,5)')
            )

            df = df.withColumn('delay_reason', 
                (F.col('act_flight_time_mins') > 0) & 
                (F.col('plnd_flight_time_mins') > 0) & 
                (F.col('act_flight_time_mins') > F.col('max_delay_allowed'))
            )

            df = df.withColumn('other_reasons', 
                F.struct(
                    F.col('other_reasons.*'), 
                    F.col('delay_reason').alias('NOT_POSSIBLE__DELAYED_FLIGHT')
                )
            )

        df = self._set_secondary_reasons(df, col_prefix)
        
        df = df.withColumn(f"{col_prefix}__net_savings", F.col('ef_savings').cast('decimal(18,5)'))

        df = df.withColumn(f"{col_prefix}__success", 
            (F.col('projected_trip_fuel') > 0) & 
            (F.col('actual_trip_fuel') < F.col('projected_trip_fuel')) & 
            (F.col(f"{col_prefix}__reason") == '')
        )
        
        df = df.withColumn(f"{col_prefix}__savings", 
            F.when(F.col(f"{col_prefix}__success"), F.col('ef_savings')).otherwise(0).cast('decimal(18,5)')
        )

        max_ef_savings = F.when(
            F.col(f"{col_prefix}__savings") > F.col('projected_trip_fuel') * 0.05, 
            F.col(f"{col_prefix}__savings")
        ).otherwise(F.col('projected_trip_fuel') * 0.05).cast('decimal(18,5)')

        df = df.withColumn(f"{col_prefix}__best_case_savings", 
            F.when(
                F.col(f"{col_prefix}__savings") > max_ef_savings, 
                F.col(f"{col_prefix}__savings")
            ).otherwise(max_ef_savings).cast('decimal(18,5)')
        )

        return df    
    
    def _calculate__Optimal_Discretionary_Fuel_Load(self, bvr_name: str, df: DataFrame, filters, *args) -> DataFrame:
        col_prefix = bvr_name.lower()
        
        required_fields = [
            'plnd_zero_fuel_weight',
            'loadsheet_zfw',
            'plnd_block_fuel',
            'act_bloff_fuel',
            'planned_fuel_burn_adjustment',
            'constant_df_buffer',
            'plnd_target_fuel',
            'act_landing_fuel',
            'planned_arrival_delay_fuel',
            'plnd_contingency_fuel'
        ]

        df, has_missing_values = self._validate_required_fields(df, bvr_name, required_fields)
        if has_missing_values:
            return df

        df = df.fillna({
            'plnd_block_fuel': 0,
            'act_bloff_fuel': 0,
            'plnd_target_fuel': 0,
            'act_landing_fuel': 0,
            'planned_arrival_delay_fuel': 0,
            'plnd_contingency_fuel': 0
        })
        df = df.withColumn('zfw_difference', 
            F.when(
                (F.col('loadsheet_zfw') == 0) | (F.col('plnd_zero_fuel_weight') == 0), 
                0
            ).otherwise(F.col('loadsheet_zfw') - F.col('plnd_zero_fuel_weight')).cast('decimal(18,5)')
        )
        df = df.withColumn('zfw_adjustment', 
            F.when(
                F.col('zfw_difference') > 0,
                F.col('zfw_difference') * F.col('planned_fuel_burn_adjustment') / 1000 + 
                (F.col('zfw_difference') * F.col('planned_fuel_burn_adjustment') / 1000) * F.col('planned_fuel_burn_adjustment') / 1000
            ).otherwise(F.col('zfw_difference') * F.col('planned_fuel_burn_adjustment') / 1000).cast('decimal(18,5)')
        )
        df = df.withColumn('adjusted_required_ramp_fuel', 
            (F.col('plnd_block_fuel') + F.col('zfw_adjustment')).cast('decimal(18,5)')
        )
        df = df.withColumn('total_expected_fuel', 
            F.col('adjusted_required_ramp_fuel').cast('decimal(18,5)')
        )
        df = df.withColumn('df_loaded', 
            (F.col('act_bloff_fuel') - F.col('adjusted_required_ramp_fuel')).cast('decimal(18,5)')
        )
        df = df.withColumn('extra_fuel_needed_to_carry_df_loaded', 
            (F.col('df_loaded') * F.col('planned_fuel_burn_adjustment') / 1000).cast('decimal(18,5)')
        )
        df = df.withColumn('revised_planned_td_fuel', 
            (F.col('plnd_target_fuel') + F.col('df_loaded') - F.col('extra_fuel_needed_to_carry_df_loaded')).cast('decimal(18,5)')
        )
        df = df.withColumn('df_needed', 
            -(F.col('act_landing_fuel') - F.col('revised_planned_td_fuel') + F.col('planned_arrival_delay_fuel') + F.col('plnd_contingency_fuel')).cast('decimal(18,5)')
        )

        df = df.withColumn('df_needed_positive_only', 
            F.when(F.col('df_needed').isNotNull(), F.greatest(F.col('df_needed'), F.lit(0))).otherwise(F.lit(0)).cast('decimal(18,5)')
        )
        df = df.withColumn('fuel_saved', 
            ((F.col('df_needed_positive_only') - F.col('df_loaded') + F.col('constant_df_buffer')) * (F.col('planned_fuel_burn_adjustment') / 10)).cast('decimal(18,5)')
        )
        df = df.withColumn('best_case_fuel_saved', 
            (F.col('constant_df_buffer') * (F.col('planned_fuel_burn_adjustment') / 1000)).cast('decimal(18,5)')
        )
        df = df.withColumn('fuel_saved', 
            F.when(F.col('fuel_saved') > F.col('best_case_fuel_saved'), F.col('best_case_fuel_saved')).otherwise(F.col('fuel_saved')).cast('decimal(18,5)')
        )
        df = df.fillna({'fuel_saved': 0, 'best_case_fuel_saved': 0})
        df = df.withColumn('discretionary_fuel_needed', F.col('df_needed_positive_only').cast('decimal(18,5)'))
        df = df.withColumn('discretionary_fuel_loaded', F.greatest(F.col('df_loaded').cast('decimal(18,5)'), F.lit(0)))
        df = df.withColumn('discretionary_fuel_loaded_penalty', F.greatest(F.col('extra_fuel_needed_to_carry_df_loaded').cast('decimal(18,5)'), F.lit(0)))

        other_reasons = {}

        other_reasons['NOT_POSSIBLE__COVID_PERIOD'] = df.withColumn(
            'covid_period',
            (F.col('ocurrence_datetime') >= F.lit('2020-03-17')) & (F.col('ocurrence_datetime') <= F.lit('2021-09-05'))
        ).select('covid_period')


        other_reasons['NOT_POSSIBLE__4_ENGINE_VEHICLE'] = df.withColumn(
            '4_engine_vehicle',
            F.col('vehicle').rlike('B74|A34')
        ).fillna(False).select('4_engine_vehicle')


        other_reasons['FAULTY_DATA__INVALID_FUEL_TANK'] = df.withColumn(
            'invalid_fuel_tank',
            F.col('act_bloff_fuel') <= 0
        ).select('invalid_fuel_tank')


        if 'plnd_tanker_fuel' in df.columns:
            df = df.withColumn('plnd_tanker_fuel', F.coalesce(F.col('plnd_tanker_fuel'), F.lit(0)))
            other_reasons['NOT_POSSIBLE__TANKERED_FLIGHT'] = df.withColumn(
                'tankered_flight',
                F.col('plnd_tanker_fuel') > 0
            ).select('tankered_flight')

        # FAULTY_DATA__OUTLIER_VALUES
        other_reasons['FAULTY_DATA__OUTLIER_VALUES'] = df.withColumn(
            'outlier_values',
            (F.col('act_bloff_fuel') < F.col('adjusted_required_ramp_fuel') * 0.8) | (F.col('act_bloff_fuel') > F.col('adjusted_required_ramp_fuel') * 1.2)
        ).select('outlier_values')


        other_reasons['NOT_POSSIBLE__DISCRETIONARY_FUEL_REQUIRED'] = df.withColumn(
            'discretionary_fuel_required',
            F.col('df_needed').isNotNull() & (F.col('df_needed') > F.col('df_loaded'))
        ).select('discretionary_fuel_required')

        # TODO Review the error type
        #df = _set_secondary_reasons(df, col_prefix, other_reasons)

        required_conditions = (df['df_loaded'] <= (df['df_needed_positive_only'] + df['constant_df_buffer'])) & (df[f"{col_prefix}__reason"] == '')

        df = df.withColumn(f"{col_prefix}__net_savings", F.col('fuel_saved'))
        df = df.withColumn(f"{col_prefix}__success",  F.when(required_conditions, True).otherwise(False))
        
        df = df.withColumn(f"{col_prefix}__savings", 
            F.when(required_conditions, F.col('fuel_saved')).otherwise(0).cast('decimal(18,5)')
        )

        df = df.withColumn(f"{col_prefix}__best_case_savings", F.col('best_case_fuel_saved').cast('decimal(18,5)'))

        return df    
    
    def _process_qualified_events(self, df, calculable_behaviours):
        df = self._set_qualified(df, calculable_behaviours)

        # Count non-qualified events
        non_qualified_count = df.filter(F.col('qualified') == False).count()

        print(f"Detected {non_qualified_count} non-qualified events")

        return df

    def run(self, df, calculable_behaviours: list[str], filters_by_behaviour: Optional[dict[str, Any]] = None) -> None:

        df = self.preprocess_df(df)

        for behaviour_name in calculable_behaviours:
            try:
                method_name = f"_calculate__{behaviour_name}"
                calculate_behaviour = getattr(self, method_name)
                
                print(f"Calculating {behaviour_name}")
                
                df = self._init_behaviour_columns(df = df, bvr_name = behaviour_name)
                df = calculate_behaviour(bvr_name = behaviour_name, df = df)
                df = self._default_nan_values(bvr_name = behaviour_name, df = df)

            except Exception as e:
                print(f"Error calculating {behaviour_name} due to: {str(e)}")
                raise
        
        bvr_columns = [col for col in df.columns if col.startswith(behaviour_name.lower())]
        print(f"Present behaviours calculated {bvr_columns}")

        df = self._process_qualified_events(df, calculable_behaviours)

        return df