
from datetime import timedelta
from src.tasks.clients.common_merge import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.ml.feature import NGram
from pyspark.sql.functions import udf
from itertools import permutations


def update_report_type(event_type, accepted_modes):
    if 'Departure' in event_type or 'Port' in event_type:
        return 'Noon In Port'
    elif not any(mode in event_type for mode in accepted_modes):
        return event_type
    else:
        return 'Noon At Sea'

def preprocess(dfs):
    accepted_modes = ['AtSeaUnderwayUsingEngine', 'InPortMoored', 'AtSeaAnchoring', 'AtSeaDrifting']
    remove_reports = ['On Hire (In Port)', 'Noon At Sea | Entering Canal | Leaving Canal']
    
    logger.info("merge data")    
    df = dfs[0]
 
    # sort by imo and date descending
    df = df.orderBy(['imo', 'date'], ascending=[True, False])

    # find the first and last index of noon report for each imo
    window_spec = Window.partitionBy('imo').orderBy(F.desc('date'))
    df = df.withColumn('row_number', F.row_number().over(window_spec))

    first_noon_df = df.filter(df['eventTypesString'].contains('Noon')).groupBy('imo').agg(F.min('row_number').alias('first_noon_index'))
    last_noon_df = df.filter(df['eventTypesString'].contains('Noon')).groupBy('imo').agg(F.max('row_number').alias('last_noon_index'))

    # Join first and last noon indices back to the main dataframe
    df = df.join(first_noon_df, on='imo', how='left')
    df = df.join(last_noon_df, on='imo', how='left')

    # Filter rows between first and last noon index
    df = df.filter((df['row_number'] >= df['first_noon_index']) & (df['row_number'] <= df['last_noon_index']))

    # Remove reports that are not in the accepted_modes
    df = df.filter(~df['eventTypesString'].isin(remove_reports) & (df['time'] <= 30)) \
            .drop('remarks') \
            .orderBy(['imo', 'date'])

    df = df.withColumn('meHours', F.coalesce(df['meHours'], F.lit(0)))

    # date format = %Y-%m-%dT%H:%M:%S.%fZ
    df = df.withColumn('date_temp1', F.to_timestamp('date', "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    df = df.withColumn('date_temp2', F.to_timestamp('date', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    df = df.withColumn('date', F.coalesce('date_temp1', 'date_temp2')).drop("date_temp1", "date_temp2")
    df = df.withColumn('update_date', F.col('date'))

    # Obtener los valores únicos de 'imo'
    unique_imos = df.select("imo").distinct()

    # Filtrar la información por IMO y encontrar las fechas donde hay un informe de Noon
    noon_reports = df.filter(df['eventTypesString'].contains('Noon')).select('imo', F.date_format('date', 'yyyy-MM-dd').alias('noon_date')).distinct()
    all_reports = df.select('imo', F.date_format('date', 'yyyy-MM-dd').alias('report_date')).distinct()

    # Encontrar las fechas donde no hay un informe de Noon
    no_noon_reports = all_reports.join(noon_reports, (all_reports['imo'] == noon_reports['imo']) & (all_reports['report_date'] == noon_reports['noon_date']), 'left_anti')

    # Añadir columna con diferencia de horas al mediodía
    df = df.withColumn('hour_diff', F.abs(F.hour('date') - 12))

    # Crear ventana para particionar por IMO y fecha de reporte y ordenar por diferencia de horas
    window_spec = Window.partitionBy('imo', F.date_format('date', 'yyyy-MM-dd')).orderBy('hour_diff')

    # Añadir columna con el ranking basado en la diferencia de horas
    df = df.withColumn('rank', F.row_number().over(window_spec))

    # Filtrar solo el reporte más cercano al mediodía para cada IMO y fecha
    closest_reports = df.filter(df['rank'] == 1).drop('hour_diff', 'rank')

    # Renombrar las columnas para evitar ambigüedades durante el join
    closest_reports = closest_reports.select([F.col(c).alias(f"closest_{c}") for c in closest_reports.columns])

    # Filtrar los reportes sin Noon y añadirles el reporte más cercano al mediodía
    reports_without_noon = no_noon_reports.join(closest_reports, 
                                                (no_noon_reports['imo'] == closest_reports['closest_imo']) & 
                                                (no_noon_reports['report_date'] == F.date_format(closest_reports['closest_date'], 'yyyy-MM-dd')),
                                                'inner')

    # Seleccionar y renombrar columnas según sea necesario
    reports_without_noon_df = reports_without_noon.select([F.col(f"{c}").alias(c) for c in closest_reports.columns])    
    
    # Registrar la función como UDF en Spark
    update_report_type_udf = F.udf(lambda event_type: update_report_type(event_type, accepted_modes), StringType())

    # Aplicar la función UDF en reports_without_noon_df
    reports_without_noon_df = reports_without_noon_df.withColumn('updated_report_type', update_report_type_udf(reports_without_noon_df['closest_eventTypesString']))

    # Seleccionar las columnas necesarias para el merge
    reports_without_noon_df = reports_without_noon_df.select(
        F.col('closest_imo').alias('imo'),
        F.col('closest_eventTypesString').alias('eventTypesString'),
        F.col('closest_date').alias('date'),
        'updated_report_type'
    )

    # Realizar el merge de vuelta con el DataFrame original
    df = df.join(reports_without_noon_df, on=['imo', 'eventTypesString', 'date'], how='left')

    # Rellenar los valores nulos en updated_report_type con los valores de eventTypesString
    df = df.withColumn('updated_report_type', F.coalesce(df['updated_report_type'], df['eventTypesString']))
    
    # Obtener los informes que no son de Noon y los informes de Noon en el mismo DataFrame
    df = df.withColumn('is_noon', F.when(df['updated_report_type'].contains('Noon'), True).otherwise(False))

    # Crear una ventana para particionar por IMO y ordenar por fecha
    window_spec = Window.partitionBy('imo').orderBy('date')

    # Añadir una columna para el siguiente informe de Noon
    df = df.withColumn('next_noon_date', F.lead('date').over(window_spec))

    # Filtrar los informes que no son de Noon y aquellos que no tienen un informe de Noon al día siguiente
    non_noon_reports = df.filter(~df['is_noon']).alias("non_noon")
    noon_reports = df.filter(df['is_noon']).alias("noon")

    # Crear una columna buffer_end en non_noon_reports
    non_noon_reports = non_noon_reports.withColumn('buffer_end', (F.date_add(F.col('non_noon.date'), 2)).cast('timestamp'))

    # Realizar el join utilizando alias para evitar ambigüedades
    no_noon_next_day = non_noon_reports.join(
        noon_reports, 
        (F.col('non_noon.imo') == F.col('noon.imo')) & 
        (F.col('noon.date') >= F.col('non_noon.date')) & 
        (F.col('noon.date') <= F.col('buffer_end')), 
        'left_anti'
    )

    # Añadir una columna para la nueva fecha, que es el día siguiente al informe no Noon
    no_noon_next_day = no_noon_next_day.withColumn('new_date', F.date_add(F.col('non_noon.date'), 1))

    # Seleccionar y renombrar columnas según sea necesario
    reports_far_from_noon_df = no_noon_next_day.select(
        F.col('non_noon.imo').alias('imo'),
        F.col('non_noon.eventTypesString').alias('eventTypesString'),
        F.col('non_noon.date').alias('original_date'),
        F.col('non_noon.updated_report_type').alias('updated_report_type'),
        F.col('new_date').alias('date')
    )

    # Realizar el merge de vuelta con el DataFrame original
    data_with_alias = df.alias("data")
    reports_far_from_noon_df_with_alias = reports_far_from_noon_df.alias("far_noon")

    # Realizar el join utilizando alias para evitar ambigüedades
    df = data_with_alias.join(
        reports_far_from_noon_df_with_alias,
        on=["imo", "eventTypesString", "date"],
        how="left"
    )

    # Rellenar los valores nulos en updated_date con los valores de date
    df = df.withColumn(
        'updated_date',
        F.coalesce(F.col('far_noon.date'), F.col('data.date'))
    )

    # Rellenar los valores nulos en updated_report_type con los valores de updated_report_type del original
    df = df.withColumn(
        'final_updated_report_type',
        F.coalesce(F.col('far_noon.updated_report_type'), F.col('data.updated_report_type'))
    )    
    
    # Crear una ventana para particionar por IMO y ordenar por fecha
    window_spec = Window.partitionBy('imo').orderBy('updated_date')
    # Filtrar los informes de Noon
    noon_reports = df.filter(F.col('final_updated_report_type').contains('Noon'))
    # Calcular el tiempo límite restando 26 horas de updated_date
    df = df.withColumn('date_minus_26h', F.expr("date_sub(updated_date, 1) + interval 2 hours - interval 26 hours"))
    # Unir los datos con los informes de Noon utilizando una ventana
    df = df.join(noon_reports, on=['imo', 'updated_date'], how='left_anti')
    # Crear la columna time_limit
    df = df.withColumn('time_limit', F.least(F.col('date_minus_26h'), F.first('updated_date').over(window_spec)))
    # Filtrar las filas dentro del límite de tiempo
    df = df.filter((F.col('updated_date') > F.col('time_limit')) & (F.col('updated_date') <= F.col('updated_date')))

    # Calcular las agregaciones necesarias
    agg_columns = [
        'aeHours1', 'aeHours2', 'aeHours3', 'aeEnergyProd', 'aeEnergyProd2', 'aeEnergyProd3', 'aeEnergyProd1', 'meEnergyProd', 'boilerHours1', 'boilerHours2', 'meHours1', 'meHours', 'meEnergyCons', 'brobUlsMgo', 'brobVlsHfo', 'combCons', 'dischargeVlsHfoCons', 'freshWaterCons', 'freshWaterRob', 'time', 'distance', 'distanceBosp', 'distanceThroughWater', 'ulsDoCons', 'aeCons', 'meCons'
    ]

    # Filtrar por operationalMode
    manoeuvring = df.filter(~F.col('operationalMode').isin(accepted_modes))
    non_manoeuvring = df.filter(F.col('operationalMode').isin(accepted_modes))

    # Calcular las sumas y medias ponderadas
    agg_data = non_manoeuvring.groupBy('imo', 'final_updated_report_type', 'updated_date').agg(
        *[F.sum(col).alias(col) for col in agg_columns],
        F.sum('meHours1').alias('sum_meHours1'),
        (F.sum(non_manoeuvring['speedBosp'] * non_manoeuvring['meHours1']) / F.sum('meHours1')).alias('speedBosp'),
        (F.sum(non_manoeuvring['windSpeedBf'] * non_manoeuvring['meHours1']) / F.sum('meHours1')).alias('windSpeedBf')
    )

    # Calcular las horas de maniobra
    manoeuvring_hours = manoeuvring.groupBy('imo', 'final_updated_report_type', 'updated_date').agg(F.sum('time').alias('manoeuvring_hours'))

    # Columnas para obtener el último valor del informe de Noon
    latest_values_columns = [
        'load', 'position', 'eventTypesString', 'ballast', 'course', 'displacement', 'distanceToGo', 'draftAft', 'draftFore', 'draftMean', 'draftMid', 'gm', 'prevPortUnLocode','nextPort', 'nextPortEta', 'nextPortTimeOffset', 'updated_date', 'nextPortUnLocode', 'repNextPortEta', 'trim', 'windDir', 'rpm', 'rpm1', 'meDailyCons', 'meLcvDailyCons', 'orderedSpeed', 'voyNum', 'airTemp', 'atmosphericPress', 'operationalMode', 'speedInstr'
    ]

    #daily_cons_columns = [col for col in df.columns if 'Daily' in col and 'Cons' in col]
    #latest_values_columns.extend(daily_cons_columns)

    # Obtener los valores más recientes
    latest_values = df.groupBy('imo', 'final_updated_report_type', 'updated_date').agg(
        *[F.first(col, ignorenulls=True).alias(col) for col in latest_values_columns]
    )

    # Unir todas las agregaciones utilizando alias para evitar ambigüedades
    agg_data = agg_data.alias("agg_data")
    manoeuvring_hours = manoeuvring_hours.alias("manoeuvring_hours")
    latest_values = latest_values.alias("latest_values")

    final_df = agg_data.join(manoeuvring_hours, on=['imo', 'final_updated_report_type', 'updated_date'], how='left').join(latest_values, on=['imo', 'final_updated_report_type', 'updated_date'], how='left')    

    # Definir el patrón de extracción para 'position'
    pattern = r'(?:Dep\.|In\sPort)\s+(.*?)(?:\s+PORT|$)'
    
    # Crear una columna con los valores extraídos
    agg_data_df = final_df.withColumn('extracted_position', F.regexp_extract('position', pattern, 1))
    
    # Limpiar y actualizar las columnas prevPortUnLocode y nextPortUnLocode
    agg_data_df = agg_data_df.withColumn('extracted_position_cleaned', 
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col('extracted_position'), ' ', ''), 'PORT', ''), '\.', ''))
    agg_data_df = agg_data_df.withColumn('prevPortUnLocode', F.when(F.col('final_updated_report_type').contains('Port'), F.col('extracted_position_cleaned').substr(1, 4)).otherwise(F.col('prevPortUnLocode')))
    agg_data_df = agg_data_df.withColumn('nextPortUnLocode', F.when(F.col('final_updated_report_type').contains('Port'), F.col('extracted_position_cleaned').substr(1, 4)).otherwise(F.col('nextPortUnLocode')))
    
    # Eliminar columnas temporales de limpieza
    agg_data_df = agg_data_df.drop('extracted_position', 'extracted_position_cleaned')
    
    # Crear una ventana para particionar por IMO y ordenar por fecha
    window_spec = Window.partitionBy('imo').orderBy('agg_data.updated_date')
    agg_data_df = agg_data_df.withColumn('date', F.col('agg_data.updated_date'))
    agg_data_df = agg_data_df.drop("agg_data.update_date")

    # Actualizar nextPortUnLocode basado en condiciones específicas
    agg_data_df = agg_data_df.withColumn('previous_next_port', F.lag('nextPortUnLocode').over(window_spec))
    agg_data_df = agg_data_df.withColumn('nextPortUnLocode', F.when(
        (F.col('final_updated_report_type') == 'Noon At Sea') & (F.col('position').startswith('EOSP')),
        F.col('previous_next_port')
    ).otherwise(F.col('nextPortUnLocode')))
    
    # Calcular las nuevas columnas
    agg_data_df = agg_data_df.withColumn('aeSfoc', F.col('aeCons') * 1000000 / F.col('aeEnergyProd'))
    agg_data_df = agg_data_df.withColumn('meSfoc', F.col('meCons') * 1000000 / F.col('meEnergyProd'))
    agg_data_df = agg_data_df.withColumn('aePower1', F.col('aeEnergyProd1') / F.col('aeHours1'))
    agg_data_df = agg_data_df.withColumn('aePower2', F.col('aeEnergyProd2') / F.col('aeHours2'))
    agg_data_df = agg_data_df.withColumn('aePower3', F.col('aeEnergyProd3') / F.col('aeHours3'))
    agg_data_df = agg_data_df.withColumn('mePower', F.col('meEnergyProd') / F.col('meHours'))
    
    # Reemplazar valores infinitos por NaN
    agg_data_df = agg_data_df.replace([float('inf'), -float('inf')], float('nan'))    
    
    dfs['pre_processed_data'] = agg_data_df
    
    return dfs