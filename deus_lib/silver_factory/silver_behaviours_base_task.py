import logging
from typing import Dict, List
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import deus_lib.utils.reason_constants as reasons
from deus_lib.silver_factory.silver_base_task import SilverBaseTask


logger = logging.getLogger(__name__)

class BaseCalculatorTask(SilverBaseTask):
    
    def __init__(self, config):
        super().__init__(config)
        
    def _init_behaviour_columns(self, df: DataFrame, bvr_name: str) -> DataFrame:
        col_prefix = bvr_name.lower()
        df = df.withColumn(f"{col_prefix}__savings", F.lit(0))
        df = df.withColumn(f"{col_prefix}__success", F.lit(False))
        df = df.withColumn(f"{col_prefix}__best_case_savings", F.lit(0))
        df = df.withColumn(f"{col_prefix}__reason", F.lit(''))
        return df
        
    def _validate_required_fields(self, df: DataFrame, bvr_name: str, required_fields=[], *args) -> Tuple[DataFrame, bool]:
        col_prefix = bvr_name.lower()
        has_missing_values = False

        for field in required_fields:
            if field not in df.columns:
                logger.warning(f"Required field '{field}' not present in df for {bvr_name.upper()} calculation")
                df = df.withColumn(f"{col_prefix}__savings", F.lit(0))
                df = df.withColumn(f"{col_prefix}__success", F.lit(False))
                df = df.withColumn(f"{col_prefix}__best_case_savings", F.lit(0))
                df = df.withColumn(f"{col_prefix}__reason", 
                                F.when(F.col(f"{col_prefix}__reason") == '', F.lit(reasons.FAULTY_DATA__MISSING_REQUIRED_VALUES))
                                .otherwise(F.col(f"{col_prefix}__reason")))
                has_missing_values = True

        return df, has_missing_values
    
    def _default_nan_values(self, bvr_name: str, df: DataFrame) -> DataFrame:
        col_prefix = bvr_name.lower()
        df = df.withColumn(f"{col_prefix}__reason", F.when(F.col(f"{col_prefix}__reason").isNull(), '').otherwise(F.col(f"{col_prefix}__reason").cast("string")))
        df = df.withColumn(f"{col_prefix}__success", F.when(F.col(f"{col_prefix}__success").isNull(), F.lit(False)).otherwise(F.col(f"{col_prefix}__success").cast("boolean")))
        df = df.withColumn(f"{col_prefix}__savings", F.when(F.col(f"{col_prefix}__savings").isNull(), F.lit(0)).otherwise(F.col(f"{col_prefix}__savings").cast("float")))
        df = df.withColumn(f"{col_prefix}__best_case_savings", F.when(F.col(f"{col_prefix}__best_case_savings").isNull(), F.lit(0)).otherwise(F.col(f"{col_prefix}__best_case_savings").cast("float")))
        return df

    def _set_secondary_reasons(self, df: DataFrame, bvr_name: str, reasons={}) -> DataFrame:
        col_prefix = bvr_name.lower()
        for other_reason, bool_condition in reasons.items():
            no_high_priority_reason = (F.col(f"{col_prefix}__reason").isNull()) | (F.col(f"{col_prefix}__reason") == '')
            df = df.withColumn(f"{col_prefix}__reason", 
                            F.when(no_high_priority_reason & bool_condition, other_reason)
                            .otherwise(F.col(f"{col_prefix}__reason")))
            df = df.withColumn(f"{col_prefix}__success", 
                            F.when(no_high_priority_reason & bool_condition, F.lit(False))
                            .otherwise(F.col(f"{col_prefix}__success")))
            df = df.withColumn(f"{col_prefix}__savings", 
                            F.when(no_high_priority_reason & bool_condition, F.lit(0))
                            .otherwise(F.col(f"{col_prefix}__savings")))
            df = df.withColumn(f"{col_prefix}__best_case_savings", 
                            F.when(no_high_priority_reason & bool_condition, F.lit(0))
                            .otherwise(F.col(f"{col_prefix}__best_case_savings")))
        return df

    def _verify_filter_exclusions(self, df: DataFrame, bvr_name: str, filters) -> DataFrame:
        for filter in filters:
            field_name = filter["field_name"]
            values = filter["values"]
            if field_name in df.columns:
                is_filtered = F.col(field_name).isin(values)
                exclusion_reason = f"{reasons.NOT_POSSIBLE__FILTERED_SETTINGS}__{field_name.upper()}"
                df = self._all_zero_by_condition(df, bvr_name, is_filtered, exclusion_reason)
        return df
    
    def _set_qualified(self, df: DataFrame, behaviours=[]) -> DataFrame:
        df = df.withColumn('qualified', F.lit(True))
        for behaviour_name in behaviours:
            is_not_qualified = F.col(f"{behaviour_name.lower()}__reason") != ''
            df = df.withColumn('qualified', 
                            F.when(is_not_qualified, F.lit(False))
                            .otherwise(F.col('qualified')))
        return df 