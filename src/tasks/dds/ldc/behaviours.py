from common_behaviours import BaseCalculatorBehaviours
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Any, Optional
import logging
import deus_lib.utils.reason_constants as reasons 
from pyspark.sql.types import IntegerType


class MariappsBehaviourCalculator(BaseCalculatorBehaviours):

    def __init__(self, customer_code: str):
        super().__init__(customer_code = customer_code)


    @staticmethod
    def preprocess_df(df: DataFrame) -> DataFrame:

        df = df.filter(F.col('voyage_time_hs') > 0) # this should be moved into gx and well as if there is no data here, stop the pipeline
        if df.isEmpty():
            raise ValueError(f"Dataframe has no data for column voyage_time_hs>0, stopping execution of the pipeline")

        return df

    def _calculate__optimal_sailing(self, df, bvr_name, *args):
        col_prefix = bvr_name.lower()
        
        #data = self._verify_filter_exclusions(data, bvr_name, filters) currently not needed
        
        required_fields = [
            'location',
            'me_foc',
            'dfoc',
            'voyage_time_hs',
            'constant_delta_fuel_consumption'
        ]
        
        # Assuming _validate_required_fields returns a tuple (DataFrame, Boolean)
        df, has_missing_values = self._validate_required_fields(df, bvr_name, required_fields)
        
        if has_missing_values:
            logging.error(f"The df has missing fields required to calculate this optimal sailing")
            return
        
        # Calculations
        df = df.withColumn('actual_fuel_consumption', F.col('me_foc') * 1000) # NOTE: me_foc is in MT (tonnes) which converted to kg is x1000, also known as total_fuel_consumption
        df = df.withColumn('ideal_fuel_used', F.col('actual_fuel_consumption') / (1 + F.col('dfoc') / 100))
        df = df.withColumn('expected_fuel_consumption', F.col('ideal_fuel_used') * (1 + F.col('constant_delta_fuel_consumption') / 100)) #also known as max_fuel_consumption
        df = df.withColumn('min_fuel_used', 
                            F.when(F.col('actual_fuel_consumption') < F.col('ideal_fuel_used'), 
                                    F.col('actual_fuel_consumption')).otherwise(F.col('ideal_fuel_used')))
        
        # Other reasons
        other_reasons = {
        reasons.NOT_POSSIBLE__NOT_AT_SEA: ~F.col('location').rlike('(?i)sea'),
        reasons.FAULTY_DATA__MISSING_REQUIRED_VALUES: F.array(*[F.col(c).isNull() for c in required_fields]).contains(True), # seems repeted from the previous step that validated required fields, which in both cases should be moved into gx
        reasons.NOT_POSSIBLE__VOYAGE_TOO_SHORT: F.col('voyage_time_hs') < 20,
        reasons.FAULTY_DATA__OUTLIER_VALUES: (F.col('expected_fuel_consumption') > 100000) | (F.col('me_foc') <= 5),
        reasons.FAULTY_DATA__INVALID_DFOC: (F.col('dfoc') >= 150) | (F.col('dfoc') < -75)
        }
        
        # Assuming _set_secondary_reasons is implemented for PySpark
        self._set_secondary_reasons(df, bvr_name, reasons= other_reasons)
        
        # More calculations
        df = df.withColumn(f"{col_prefix}__net_savings", F.col('expected_fuel_consumption') - F.col('actual_fuel_consumption'))
        
        # Success conditions
        df = df.withColumn(f"{col_prefix}__success", 
                            (F.col(f"{col_prefix}__net_savings") > 0) & 
                            (F.col('dfoc') < F.col('constant_delta_fuel_consumption')) & 
                            (F.col(f"{col_prefix}__reason") == ''))
        
        df = df.withColumn(f"{col_prefix}__savings", 
                            F.when(F.col(f"{col_prefix}__success"), F.col(f"{col_prefix}__net_savings")).otherwise(0))
        
        foc_best_savings_col = F.col('expected_fuel_consumption')-F.col('min_fuel_used')
        
        df = df.withColumn(f"{col_prefix}__best_case_savings", 
                            F.when((F.col('expected_fuel_consumption') - F.col('min_fuel_used')) > 0, foc_best_savings_col).otherwise(0))
                
        # Placeholder for MariApps predictor model
        df = df.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None))
        
        return df


    def _calculate__engine_maintenance_optimisation(self, df, bvr_name, *args):
        col_prefix = bvr_name.lower()
        
        required_fields = [
            'location', 'me_sfoc', 'me_hours', 'me_power', 'delta_sfoc',
            'voyage_time_hs', 'constant_emo_delta_fuel'
        ]
        
        df, has_missing_values = self._validate_required_fields(df, bvr_name, required_fields)
        
        if has_missing_values:
            logging.error(f"The df has missing fields required to calculate Engine Maintenance Optimisation")
            return df
                
        # Other reasons
        other_reasons = {
            reasons.NOT_POSSIBLE__NOT_AT_SEA: ~F.col('location').rlike('(?i)sea'),
            reasons.FAULTY_DATA__MISSING_REQUIRED_VALUES: F.array(*[F.col(c).isNull() for c in required_fields]).contains(True),
            reasons.NOT_POSSIBLE__VOYAGE_TOO_SHORT: F.col('voyage_time_hs') < 6,
            reasons.FAULTY_DATA__INVALID_RUNNING_HOURS: (F.col('me_hours') < 20) | (F.col('me_hours') > 30),
            reasons.FAULTY_DATA__INVALID_KWHRS: F.col('me_power') > 10000,
            reasons.FAULTY_DATA__INVALID_SFOC: (F.col('me_sfoc').isNull()) | (F.col('me_sfoc') < 100) | (F.col('me_sfoc') > 400),
            reasons.FAULTY_DATA__INVALID_DELTA_SFOC: (F.col('delta_sfoc').isNull()) | (F.col('delta_sfoc') > 99) | (F.col('delta_sfoc') < -99),
            reasons.FAULTY_DATA__OUTLIER_VALUES: F.col('me_power') > 300000
        }
        
        df = self._set_secondary_reasons(df, bvr_name, reasons=other_reasons)
        
        # Calculations
        me_total_power_col = (F.col('me_power') * F.col('me_hours'))
        total_fuel_used_col = (F.col('me_sfoc') * me_total_power_col / 1000)
        
        emo_threshold_init = (F.col('me_sfoc') / (1 + F.col('delta_sfoc') / 100))
        df = df.withColumn('emo_threshold', #also known as max_emo_threshold_with_buffer
                        emo_threshold_init * (1 + (F.col('constant_emo_delta_fuel') / 100)))
        ideal_fuel_used = (F.col('emo_threshold') * me_total_power_col / 1000) #convert to kgs
        
        under_sfoc_threshold_col = (F.col('me_sfoc') <= F.col('emo_threshold'))
        delta_fuel_used_col  =  (ideal_fuel_used - total_fuel_used_col)
        not_excluded_col = (F.col(f"{col_prefix}__reason") == '')
        succeded_col = under_sfoc_threshold_col & (F.col(f"{col_prefix}__reason") == '')

        df = df.withColumn(f"{col_prefix}__success", succeded_col)
        df = df.withColumn(f"{col_prefix}__net_savings", 
                        F.when(not_excluded_col, delta_fuel_used_col).otherwise(0))
        df = df.withColumn(f"{col_prefix}__savings", 
                        F.when(succeded_col & (delta_fuel_used_col > 0), delta_fuel_used_col).otherwise(0))
        df = df.withColumn(f"{col_prefix}__best_case_savings", F.col(f"{col_prefix}__savings")) #weird best case savings is savings but okay
        df = df.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None))
        
        return df
    

    def _genrunning(self, ae1, ae2, ae3, ae4):
        """
        This function calculates the number of actual generators that were running for that noon report.
        It returns a Column that can be used in a withColumn operation.
        """
        return (
            F.when(F.col(ae1) > 0, 1).otherwise(0) +
            F.when(F.col(ae2) > 0, 1).otherwise(0) +
            F.when(F.col(ae3) > 0, 1).otherwise(0) +
            F.when(F.col(ae4) > 0, 1).otherwise(0)
        ).cast(IntegerType())
    

    def _energytotal(self, ld):
        """
        This function calculates whether the energy demand is within the max theoretical energy output
        of N generators based on the max MCR, where N is the number of generators running.
        """
        epsilon = F.lit(1)
        
        # Sort the max loads of aux engines
        sorted_loads = F.array_sort(F.array(
            F.col('aux_engine1_max_load'),
            F.col('aux_engine2_max_load'),
            F.col('aux_engine3_max_load'),
            F.col('aux_engine4_max_load')
        ))

        # Calculate max energy outputs for different numbers of generators
        one_max_gen = sorted_loads.getItem(3) * F.col('aux_engine1_const') * (F.col(ld) + epsilon)
        two_max_gen = (sorted_loads.getItem(3) + sorted_loads.getItem(2)) * F.col('aux_engine1_const') * (F.col(ld) + epsilon)
        three_max_gen = (sorted_loads.getItem(3) + sorted_loads.getItem(2) + sorted_loads.getItem(1)) * F.col('aux_engine1_const') * (F.col(ld) + epsilon)
        four_max_gen = sorted_loads.cast("array<double>").reduce(lambda x, y: x + y) * F.col('aux_engine1_const') * (F.col(ld) + epsilon)

        # Determine if energy demand is within the appropriate range based on number of generators running
        return F.when(
            (F.col('GenRunning') == 1) & (F.col('Edemand') <= one_max_gen), 1
        ).when(
            (F.col('GenRunning') == 2) & (F.col('Edemand') > one_max_gen) & (F.col('Edemand') <= two_max_gen), 1
        ).when(
            (F.col('GenRunning') == 3) & (F.col('Edemand') > two_max_gen) & (F.col('Edemand') <= three_max_gen), 1
        ).when(
            (F.col('GenRunning') == 4) & (F.col('Edemand') > three_max_gen) & (F.col('Edemand') <= four_max_gen), 1
        ).otherwise(0).cast(IntegerType())
    
    
    def _calculate__efficient_auxiliary_engine_use(self, df, bvr_name, *args):
        col_prefix = bvr_name.lower()
        
        required_fields = [
            'location', 'aeu', 'sfocisoae', 'constant_aeu_lower', 'constant_aeu_upper',
            'ae1_running_hours', 'ae2_running_hours', 'ae3_running_hours', 'ae4_running_hours',
            'ae1_electric_power_kw', 'ae2_electric_power_kw', 'ae3_electric_power_kw', 'ae4_electric_power_kw',
            'aux_engine1_max_load', 'aux_engine2_max_load', 'aux_engine3_max_load', 'aux_engine4_max_load',
            'aux_engine1_const', 'aux_engine2_const', 'aux_engine3_const', 'aux_engine4_const',
            'log_duration'
        ]
        
        df, has_missing_values = self._validate_required_fields(df, bvr_name, required_fields)
        
        if has_missing_values:
            logging.error(f"The df has missing fields required to calculate Efficient Auxiliary Engine Use")
            return df

        eta = F.lit(0.96)

        # Other reasons
        other_reasons = {
            reasons.FAULTY_DATA__MISSING_REQUIRED_VALUES: F.array(*[F.col(c).isNull() for c in required_fields]).contains(True),
            reasons.NOT_POSSIBLE__VOYAGE_TOO_SHORT: F.col('log_duration') < 20,
            reasons.NOT_POSSIBLE__EAEU_OVER_THRESHOLD: F.col('aeu') > 100,
            reasons.FAULTY_DATA__EAEU_NOT_USED: (F.col('aeu') == 0) | ((F.col('ae1_running_hours') + F.col('ae2_running_hours') + F.col('ae3_running_hours') + F.col('ae4_running_hours')) <= 0),
            reasons.FAULTY_DATA__INVALID_SFOC: (F.col('sfocisoae') < 100) | (F.col('sfocisoae') > 400)
        }
        
        df = self._set_secondary_reasons(df, bvr_name, reasons=other_reasons)

        # Calculate MCR for each auxiliary engine
        for i in range(1, 5):
            df = df.withColumn(f'AE{i}_MCR', F.col(f'aux_engine{i}_const') * F.col(f'aux_engine{i}_max_load'))
            df = df.withColumn(f'AE{i}_Load', F.col(f'ae{i}_electric_power_kw') / F.col(f'aux_engine{i}_max_load'))

        # Calculate the actual energy demand
        df = df.withColumn('Edemand', 
            F.sum([F.col(f'ae{i}_electric_power_kw') * F.col(f'ae{i}_running_hours') / eta for i in range(1, 5)]))

        # Calculate GenRunning and EnergyTotal
        df = df.withColumn('GenRunning', self._genrunning('ae1_running_hours', 'ae2_running_hours', 'ae3_running_hours', 'ae4_running_hours'))
        df = df.withColumn('EnergyTotal', self._energytotal('log_duration'))

        # Calculate denominator for AEU equation
        denom_col = (F.sum([F.col(f'aux_engine{i}_const') * F.col(f'AE{i}_MCR') * F.col(f'ae{i}_running_hours') for i in range(1, 5)]))

        # Calculate total_ae_running_hours and aeu
        df = df.withColumn('total_ae_running_hours', 
            F.sum([F.col(f'ae{i}_running_hours') for i in range(1, 5)]))
        
        df = df.withColumn('aeu', 
            F.when((F.col('EnergyTotal') == 1) | (F.col('Edemand') / denom_col >= 1) | (F.col('total_ae_running_hours') <= F.col('log_duration')),
                F.lit(100))
            .otherwise((F.col('Edemand') / denom_col) * 100))

        # Calculate net_savings, success, savings, and best_case_savings
        df = df.withColumn(f"{col_prefix}__net_savings", 
            ((F.col('aeu') - F.col('constant_aeu_lower')) / 100) * F.col('sfocisoae') * F.col('Edemand') / 1000)
        
        df = df.withColumn(f"{col_prefix}__success", 
            (F.col(f"{col_prefix}__reason") == '') & (F.col('aeu') >= F.col('constant_aeu_lower')))
        
        df = df.withColumn(f"{col_prefix}__savings", 
            F.when(F.col(f"{col_prefix}__success"), F.round(F.col(f"{col_prefix}__net_savings"), 2)).otherwise(0))
        
        df = df.withColumn(f"{col_prefix}__best_case_savings", F.col(f"{col_prefix}__savings"))
        
        # Placeholder for MariApps predictor model
        df = df.withColumn(f"{col_prefix}__succeeded_predicted", F.lit(None))

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
        


