import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from src.tasks.clients.common_merge import *

def merge(dfs):
    logger.info("merge data")
    
    source_df, target_df = dfs["flight_data"], dfs["fuelburn_data"]
    
    source_df = source_df.withColumn("ocurrence_datetime", F.to_timestamp("ocurrence_datetime"))
    target_df = target_df.withColumn("ocurrence_datetime", F.to_timestamp("ocurrence_datetime"))
    
    source_df = source_df.withColumnRenamed("ocurrence_datetime", "source_ocurrence_datetime")
    target_df = target_df.withColumnRenamed("ocurrence_datetime", "target_ocurrence_datetime")
    
    source_df = source_df.withColumnRenamed("sourcefilename", "source_sourcefilename")
    target_df = target_df.withColumnRenamed("sourcefilename", "target_sourcefilename")

    source_df = source_df.withColumnRenamed("filepath", "source_filepath")
    target_df = target_df.withColumnRenamed("filepath", "target_filepath")        
    
    source_df = source_df.withColumnRenamed("upload_date", "source_upload_date")
    target_df = target_df.withColumnRenamed("upload_date", "target_upload_date")            
    
    source_df = source_df.withColumnRenamed("ingestiondatetime", "source_ingestiondatetime")
    target_df = target_df.withColumnRenamed("ingestiondatetime", "target_ingestiondatetime")
    
    target_df = target_df.withColumnRenamed("taxi_out_minutes_measurement", "target_taxi_out_minutes_measurement")
    target_df = target_df.withColumnRenamed("taxi_in_minutes_measurement", "target_taxi_in_minutes_measurement")
    target_df = target_df.withColumnRenamed("origin", "target_origin")
    target_df = target_df.withColumnRenamed("destination", "target_destination")
    target_df = target_df.withColumnRenamed("identifier", "target_identifier")
    target_df = target_df.withColumnRenamed("ingestionjobrunid", "target_ingestionjobrunid")
    
    target_df = target_df.drop("status")
    target_df = target_df.drop("event_id")
    target_df = target_df.drop("customer_code")

    source_df = source_df.orderBy("source_ocurrence_datetime")
    target_df = target_df.orderBy("target_ocurrence_datetime")
    
    window_spec = Window.partitionBy("aircraft_reg_code").orderBy(F.col("source_ocurrence_datetime").cast("long")).rangeBetween(-1800, 1800)
    
    target_df = target_df.withColumn("target_time", F.col("target_ocurrence_datetime").cast("long"))
    source_df = source_df.withColumn("source_time", F.col("source_ocurrence_datetime").cast("long"))
    
    joined_df = source_df.join(target_df, on="aircraft_reg_code", how="left")
    joined_df = joined_df.withColumn("time_diff", F.abs(F.col("source_time") - F.col("target_time")))
    joined_df = joined_df.filter(joined_df["time_diff"] <= 3600)
    joined_df = joined_df.withColumn("min_time_diff", F.min("time_diff").over(window_spec))
    joined_df = joined_df.filter(F.col("time_diff") == F.col("min_time_diff"))
    
    joined_df = joined_df.withColumn('customer_code_upper', F.upper(F.col('customer_code')))
    joined_df = joined_df.withColumn('ocurrence_datetime_numeric', F.regexp_replace(F.col('source_ocurrence_datetime'), r'\D', ''))
    
    joined_df = joined_df.withColumnRenamed("source_ocurrence_datetime", "ocurrence_datetime")
    
    joined_df = joined_df.dropDuplicates(["ocurrence_datetime", "aircraft_reg_code"])    
    
    final_columns = [col for col in joined_df.columns if not col.endswith("_y")]
    
    logger.info(f"Final columns: {final_columns}")
    
    joined_df = joined_df.select(final_columns)
    
    return joined_df 