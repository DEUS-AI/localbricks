from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType
from pyspark.sql.window import Window

def merge(dfs):
    df_noon_report = dfs["blob_noon_report"]
    df_fuel = dfs["blob_fuel_consumption"]
    df_optimal_engine = dfs["blob_optimal_engine"]

    df_fuel = df_fuel.withColumn('merge_date', F.to_date(F.col('REPORT_DATE')))
    df_noon_report = df_noon_report.withColumn('noon_merge_date', F.to_date(F.col('DimReportDateId')))
    df_optimal_engine = df_optimal_engine.withColumn('op_merge_date', F.to_date(F.col('REPORT_DATE')))
    
    df_fuel = df_fuel.withColumnRenamed('shipname', 'fuel_shipname')
    df_noon_report = df_noon_report.withColumnRenamed('shipname', 'noon_shipname')
    df_optimal_engine = df_optimal_engine.withColumnRenamed('SHIPNAME', 'op_shipname')
    
    
    merged_df = df_fuel.join(df_noon_report, 
                            (df_fuel['merge_date'] == df_noon_report['noon_merge_date']) & 
                            (df_fuel['fuel_shipname'] == df_noon_report['noon_shipname']), 
                            'inner')
    
    merged_df = merged_df.join(df_optimal_engine, 
                            (merged_df['merge_date'] == df_optimal_engine['op_merge_date']) & 
                            (merged_df['fuel_shipname'] == df_optimal_engine['op_shipname']), 
                            'inner')
    
    merged_df = merged_df.withColumnRenamed('fuel_shipname', 'shipname')
    merged_df = merged_df.drop('noon_merge_date', 'op_merge_date', 'noon_shipname', 'op_shipname', 'REPORT_DATE')
    merged_df = merged_df.withColumnRenamed('merge_date', 'report_date')        
    
    dfs['merged_data'] = merged_df
    
    return dfs