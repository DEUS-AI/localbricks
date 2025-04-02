import logging
from typing import Dict, List, Tuple, Optional, Any
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from deus_lib.utils.reason_constants import *

logger = logging.getLogger(__name__)

class BaseCalculatorBehaviours:

    def __init__(self, dds_code: str):
        logging.info(f"Starting base calculator for DDS code {dds_code}")
        self.dds_code = dds_code

    def _init_behaviour_columns(self, df, bvr_name: str) -> DataFrame:
        """Initialize behaviour columns with default values.
        
        Args:
            df: Input DataFrame
            bvr_name: Name of the behaviour
            
        Returns:
            DataFrame with initialized columns
        """
        try:
            col_prefix = bvr_name.lower()
            df = df.withColumn(f"{col_prefix}__savings", F.lit(0))
            df = df.withColumn(f"{col_prefix}__success", F.lit(False))
            df = df.withColumn(f"{col_prefix}__best_case_savings", F.lit(0))
            df = df.withColumn(f"{col_prefix}__reason", F.lit(''))
            return df
        except Exception as e:
            logger.error(f"Error initializing behaviour columns for {bvr_name}: {str(e)}")
            raise

    def _validate_required_fields(self, df, bvr_name: str, required_fields=[], *args) -> Tuple[DataFrame, bool]:
        """Validate required fields exist in DataFrame.
        
        Args:
            df: Input DataFrame
            bvr_name: Name of the behaviour
            required_fields: List of required field names
            
        Returns:
            Tuple of (DataFrame with validation columns, boolean indicating if validation failed)
        """
        try:
            col_prefix = bvr_name.lower()
            has_missing_values = False
            missing_fields = []
            
            for field in required_fields:
                if field not in df.columns:
                    missing_fields.append(field)
                    has_missing_values = True
            
            if has_missing_values:
                logger.warning(f"Required fields {missing_fields} not present in df for {bvr_name.upper()} calculation")
                df = df.withColumn(f"{col_prefix}__savings", F.lit(0))
                df = df.withColumn(f"{col_prefix}__success", F.lit(False))
                df = df.withColumn(f"{col_prefix}__best_case_savings", F.lit(0))
                df = df.withColumn(f"{col_prefix}__reason", 
                                F.when(F.col(f"{col_prefix}__reason") == '', F.lit(FAULTY_DATA__MISSING_REQUIRED_VALUES))
                                .otherwise(F.col(f"{col_prefix}__reason")))
            
            return df, has_missing_values
        except Exception as e:
            logger.error(f"Error validating required fields for {bvr_name}: {str(e)}")
            raise

    def _set_secondary_reasons(self, df: DataFrame, bvr_name: str, reasons: Dict) -> DataFrame:
        """Set secondary reasons for behaviour failures.
        
        Args:
            df: Input DataFrame
            bvr_name: Name of the behaviour
            reasons: Dictionary of reason codes and conditions
            
        Returns:
            DataFrame with updated reason codes
        """
        try:
            col_prefix = bvr_name.lower()
            for reason_code, condition in reasons.items():
                df = df.withColumn(f"{col_prefix}__reason",
                    F.when(
                        (condition) & (F.col(f"{col_prefix}__reason") == ''),
                        F.lit(reason_code)
                    ).otherwise(F.col(f"{col_prefix}__reason"))
                )
            return df
        except Exception as e:
            logger.error(f"Error setting secondary reasons for {bvr_name}: {str(e)}")
            raise

    def _default_nan_values(self, df: DataFrame, bvr_name: str) -> DataFrame:
        """Replace NaN values with defaults for behaviour columns.
        
        Args:
            df: Input DataFrame
            bvr_name: Name of the behaviour
            
        Returns:
            DataFrame with NaN values replaced
        """
        try:
            col_prefix = bvr_name.lower()
            df = df.fillna({
                f"{col_prefix}__savings": 0,
                f"{col_prefix}__success": False,
                f"{col_prefix}__best_case_savings": 0,
                f"{col_prefix}__reason": ''
            })
            return df
        except Exception as e:
            logger.error(f"Error defaulting NaN values for {bvr_name}: {str(e)}")
            raise

    def _process_qualified_events(self, df: DataFrame, calculable_behaviours: List[str]) -> DataFrame:
        """Process qualified events for behaviours.
        
        Args:
            df: Input DataFrame
            calculable_behaviours: List of behaviour names
            
        Returns:
            DataFrame with qualified events processed
        """
        try:
            df = self._set_qualified(df, calculable_behaviours)
            non_qualified_count = df.filter(F.col('qualified') == False).count()
            logger.info(f"Detected {non_qualified_count} non-qualified events")
            return df
        except Exception as e:
            logger.error(f"Error processing qualified events: {str(e)}")
            raise

    def run(self, df: DataFrame, calculable_behaviours: List[str], filters_by_behaviour: Optional[Dict[str, Any]] = None) -> DataFrame:
        """Run behaviour calculations.
        
        Args:
            df: Input DataFrame
            calculable_behaviours: List of behaviour names
            filters_by_behaviour: Optional filters for behaviours
            
        Returns:
            DataFrame with calculated behaviours
        """
        try:
            df = self.preprocess_df(df)
            
            for behaviour_name in calculable_behaviours:
                try:
                    method_name = f"_calculate__{behaviour_name}"
                    calculate_behaviour = getattr(self, method_name)
                    logger.info(f"Calculating {behaviour_name}")
                    
                    df = self._init_behaviour_columns(df=df, bvr_name=behaviour_name)
                    df = calculate_behaviour(bvr_name=behaviour_name, df=df)
                    df = self._default_nan_values(df=df, bvr_name=behaviour_name)
                    
                    bvr_columns = [col for col in df.columns if col.startswith(behaviour_name.lower())]
                    logger.info(f"Present behaviours calculated: {bvr_columns}")
                    
                except AttributeError:
                    logger.error(f"Behaviour calculation method {method_name} not found")
                    raise
                except Exception as e:
                    logger.error(f"Error calculating {behaviour_name}: {str(e)}")
                    raise
            
            df = self._process_qualified_events(df, calculable_behaviours)
            return df
            
        except Exception as e:
            logger.error(f"Error running behaviour calculations: {str(e)}")
            raise

    def _all_zero_by_condition(self, df, bvr_name: str, bool_condition=True, failed_reason='') -> DataFrame:
        col_prefix = bvr_name.lower()

        df = df.withColumn(f"{col_prefix}__reason", F.when(bool_condition, failed_reason).otherwise(F.col(f"{col_prefix}__reason")))
        df = df.withColumn(f"{col_prefix}__success", F.when(bool_condition, False).otherwise(F.col(f"{col_prefix}__success")))
        df = df.withColumn(f"{col_prefix}__savings", F.when(bool_condition, 0).otherwise(F.col(f"{col_prefix}__savings")))
        df = df.withColumn(f"{col_prefix}__best_case_savings", F.when(bool_condition, 0).otherwise(F.col(f"{col_prefix}__best_case_savings")))

        return df

    def _verify_filter_exclusions(self, df, bvr_name: str, filters) -> DataFrame:
        for filter in filters:
            field_name = filter["field_name"]
            values = filter["values"]
            if field_name in df.columns:
                is_filtered = F.col(field_name).isin(values)
                exclusion_reason = f"{NOT_POSSIBLE__FILTERED_SETTINGS}__{field_name.upper()}"
                self._all_zero_by_condition(df, bvr_name, is_filtered, exclusion_reason)

    def _set_qualified(self, df, behaviours=[]) -> DataFrame:
        df = df.withColumn('qualified', F.lit(True))
        for behaviour_name in behaviours:
            is_not_qualified = F.col(f"{behaviour_name.lower()}__reason") != ''
            df = df.withColumn('qualified', 
                            F.when(is_not_qualified, F.lit(False))
                            .otherwise(F.col('qualified')))
        return df
