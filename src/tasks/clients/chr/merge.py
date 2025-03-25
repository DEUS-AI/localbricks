
from datetime import timedelta
from src.tasks.clients.common_merge import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.ml.feature import NGram
from pyspark.sql.functions import udf
from itertools import permutations


# Definir las funciones UDF
def sanitize_full_name(full_name):
    return ' '.join(w for w in list(dict.fromkeys(str(full_name).split())))

def capitalize_name(full_name):
    return ' '.join(w.capitalize() for w in str(full_name).split())

def merge(dfs):
    api_data = dfs["pre_processed_data"]
    crew_rotation_data = dfs["crew_rotation"]
    
    api_data = api_data.withColumnRenamed("data.captain", "captain")
    api_data = api_data.withColumnRenamed("data.chiefEngineer", "chiefEngineer")
    
    sanitize_full_name_udf = F.udf(sanitize_full_name, StringType())
    capitalize_name_udf = F.udf(capitalize_name, StringType())
    
    # Crear la columna full_name en crew_rotation_data
    crew_rotation_data = crew_rotation_data.withColumn(
        'full_name',
        F.concat(F.split(F.col('firstname'), ' ')[0], F.lit(' '), F.col('lastname'))
    )
    
    # Aplicar las transformaciones a full_name
    crew_rotation_data = crew_rotation_data.withColumn('full_name', sanitize_full_name_udf(F.col('full_name')))
    crew_rotation_data = crew_rotation_data.withColumn('full_name', F.lower(F.regexp_replace(F.col('full_name'), r'\.', '')).alias('full_name'))
    
    tomorrow = (datetime.datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    crew_rotation_data = crew_rotation_data.withColumn('start_date', F.to_date(F.col('start_date')))
    crew_rotation_data = crew_rotation_data.withColumn('end_date', F.coalesce(F.to_date(F.col('end_date')), F.lit(tomorrow).cast(DateType())))
    
    api_data = api_data.withColumn('date', F.to_timestamp(F.col('date'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    api_data = api_data.filter(F.col('date').isNotNull() & F.col('captain').isNotNull() & F.col('chiefEngineer').isNotNull())
    
    api_data = api_data.withColumn('captain', sanitize_full_name_udf(F.col('captain')))
    api_data = api_data.withColumn('captain', F.lower(F.regexp_replace(F.col('captain'), r'\.', '')).alias('captain'))
    api_data = api_data.withColumn('chiefEngineer', sanitize_full_name_udf(F.col('chiefEngineer')))
    api_data = api_data.withColumn('chiefEngineer', F.lower(F.regexp_replace(F.col('chiefEngineer'), r'\.', '')).alias('chiefEngineer'))
    
    api_data = api_data.withColumn('date', F.to_date(F.col('date')))
    
    api_data = api_data.dropDuplicates(['imo', 'date'])
    api_data = api_data.orderBy(['imo', 'date'], ascending=[True, False]).withColumn("rank", F.row_number().over(Window.partitionBy("imo").orderBy(F.desc("date"))))
    api_data = api_data.filter(F.col("rank") == 1).drop("rank")
    
    crew_rotation_data = crew_rotation_data.withColumnRenamed("imo", "crew_imo")

    merged_df = api_data.join(
        crew_rotation_data,
        (api_data['imo'] == crew_rotation_data['crew_imo']) &
        (crew_rotation_data['start_date'] <= api_data['date']) &
        (api_data['date'] <= crew_rotation_data['end_date']),
        'inner'
    ).drop("crew_imo")
    
    merged_df = merged_df.withColumn('rank', F.lower(F.col("rank")))

    captain_df = merged_df.filter(merged_df['rank'] == 'master').select(
        'imo',
        'date',
        'ship_name',
        'full_name',
        'discharge_book_number'
    )
    captain_df = captain_df.withColumnRenamed('full_name', 'crew__captain') 
    captain_df = captain_df.withColumnRenamed('discharge_book_number', 'crew__captainDBN')
    captain_df = captain_df.withColumnRenamed('imo', 'c_imo')
    captain_df = captain_df.withColumnRenamed('date', 'c_date')    
    
    chief_engineer_df = merged_df.filter(merged_df['rank'] == 'chief engineer').select(
        'imo',
        'date',
        'ship_name',
        'full_name',
        'discharge_book_number'
    )
    chief_engineer_df = chief_engineer_df.withColumnRenamed('full_name', 'crew__chiefEngineer')
    chief_engineer_df = chief_engineer_df.withColumnRenamed('discharge_book_number', 'crew__chiefEngineerDBN')
    chief_engineer_df = chief_engineer_df.withColumnRenamed('imo', 'e_imo')
    chief_engineer_df = chief_engineer_df.withColumnRenamed('date', 'e_date')    
    
    result_df = api_data.join(
        captain_df,
         (api_data['imo'] == captain_df['c_imo']) & (api_data['date'] == captain_df['c_date']),    
        'left'
    ).join(
        chief_engineer_df,
         (api_data['imo'] == chief_engineer_df['e_imo']) & (api_data['date'] == chief_engineer_df['e_date']),
        'left'
    ).drop("c_imo", "c_date", "e_imo", "e_date")

    # Añadir y capitalizar nombres de stormgeo
    result_df = result_df.withColumn('stormgeo__captain', capitalize_name_udf(F.col('captain')))
    result_df = result_df.withColumn('stormgeo__chiefEngineer', capitalize_name_udf(F.col('chiefEngineer')))

    # Añadir columnas adicionales con valores nulos por defecto
    result_df = result_df.withColumn('captain__status', F.lit(None).cast(StringType()))
    result_df = result_df.withColumn('chiefEngineer__status', F.lit(None).cast(StringType()))    
    
    result_df.dropDuplicates()
    
    return result_df