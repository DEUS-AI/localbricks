from enum import Enum
from src.tasks.clients.common_merge import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType
from functools import reduce
import json
import os

@F.udf(returnType=IntegerType())
def genrunning_udf(gen_rh):
    return sum(1 for rh in gen_rh if rh is not None and rh > 0)
    
@F.udf(returnType=IntegerType())
def energytotal_udf(genrunning, edemand, max_load_1, max_load_2, max_load_3, max_load_4, const):
    lst = sorted([max_load_1, max_load_2, max_load_3, max_load_4])
    log_duration = 24
    epsilon = 1
    one_max_gen = lst[-1] * const * (log_duration + epsilon)
    two_max_gen = (lst[-1] + lst[-2]) * const * (log_duration + epsilon)
    three_max_gen = (lst[-1] + lst[-2] + lst[-3]) * const * (log_duration + epsilon)
    four_max_gen = sum(lst) * const * (log_duration + epsilon)
    if genrunning == 1 and edemand <= one_max_gen:
        return 1
    elif genrunning == 2 and one_max_gen < edemand <= two_max_gen:
        return 1
    elif genrunning == 3 and two_max_gen < edemand <= three_max_gen:
        return 1
    elif genrunning == 4 and three_max_gen < edemand <= four_max_gen:
        return 1
    else:
        return 0

class RequiredContinousDataFields(Enum):
    shipname = "SHIPNAME"
    date = "EVENTDATE"
    time = "EVENTTIME"
    gen_1_kwhrs = "DG 1 KWH PRODUCED"
    gen_2_kwhrs = "DG 2 KWH PRODUCED"
    gen_3_kwhrs = "DG 3 KWH PRODUCED"
    gen_4_kwhrs = "DG 4 KWH PRODUCED"
    fuel_flow_cubic = "FUELFLOWCONSUMPTION"
    
def load_vessel_config():
    """Load vessel configuration from JSON file."""
    config_path = os.path.join(os.path.dirname(__file__), 'vessel_config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

# Load vessel configuration from external file
mcr_dict = load_vessel_config()

def update_report_type(event_type, accepted_modes):
    if 'Departure' in event_type or 'Port' in event_type:
        return 'Noon In Port'
    elif not any(mode in event_type for mode in accepted_modes):
        return event_type
    else:
        return 'Noon At Sea'

def preprocess(dfs):
    
    dfs["blob_noon_report"] = process_noon_data(dfs["blob_noon_report"])
    dfs["blob_fuel_consumption"] = process_fuel_data(dfs["blob_fuel_consumption"])
    dfs["blob_optimal_engine"] = process_fuel_data(dfs["blob_optimal_engine"])
        
    return dfs

def process_continuous_data(data: DataFrame) -> DataFrame:
    data = data.select([F.col(c).alias(c.upper()) for c in data.columns])
    
    # Arbitrary constant defined from BSM report
    eta = 0.96

    # Filter out all non-iot ships
    iot_vessel_names = [
        "Stolt Breland",
        "Stolt Achievement",
        "Stolt Tenacity",
        "Stolt Concept",
    ]
    df = data.filter(F.col("SHIPNAME").isin(iot_vessel_names))

    # Convert columns to float
    float_columns = [
        "DG 1 RUNNING HOURS",
        "DG 2 RUNNING HOURS",
        "DG 3 RUNNING HOURS",
        "DG 4 RUNNING HOURS",
        "DG 1 KWH PRODUCED",
        "DG 2 KWH PRODUCED",
        "DG 3 KWH PRODUCED",
        "DG 4 KWH PRODUCED",
        "FUELFLOWCONSUMPTION",
    ]

    for col in float_columns:
        df = df.withColumn(col, F.col(col).cast(FloatType()))

    # Sort values by ship name and datetime
    df = df.orderBy(
        RequiredContinousDataFields.shipname.value, "EVENTDATE", "EVENTTIME"
    )

    # Find not IOT ships, keep them apart and merge them back later
    nonIOT = df.filter(
        ~F.col(RequiredContinousDataFields.shipname.value).isin(
            list(mcr_dict.keys())
        )
    )
    nonIOT = nonIOT.withColumnRenamed("EVENTDATE", "REPORT_DATE")

    # Filter IOT ships
    df = df.filter(
        F.col(RequiredContinousDataFields.shipname.value).isin(
            list(mcr_dict.keys())
        )
    )

    # Calculate difference in running hours
    window = Window.partitionBy(
        RequiredContinousDataFields.shipname.value
    ).orderBy("EVENTDATE", "EVENTTIME")
    for i in range(1, 5):
        df = df.withColumn(
            f"gen_{i}_rh",
            F.round(
                F.col(f"DG {i} RUNNING HOURS")
                - F.lag(f"DG {i} RUNNING HOURS").over(window),
                1,
            ),
        )

    df = df.withColumn(
        RequiredContinousDataFields.gen_1_kwhrs.value,
        F.when(F.col("gen_1_rh") == 0, 0).otherwise(
            F.col(RequiredContinousDataFields.gen_1_kwhrs.value)
        ),
    )

    df = df.withColumn(
        RequiredContinousDataFields.gen_2_kwhrs.value,
        F.when(F.col("gen_2_rh") == 0, 0).otherwise(
            F.col(RequiredContinousDataFields.gen_2_kwhrs.value)
        ),
    )

    df = df.withColumn(
        RequiredContinousDataFields.gen_3_kwhrs.value,
        F.when(F.col("gen_3_rh") == 0, 0).otherwise(
            F.col(RequiredContinousDataFields.gen_3_kwhrs.value)
        ),
    )

    df = df.withColumn(
        RequiredContinousDataFields.gen_4_kwhrs.value,
        F.when(F.col("gen_4_rh") == 0, 0).otherwise(
            F.col(RequiredContinousDataFields.gen_4_kwhrs.value)
        ),
    )

    df = df.withColumn(
        RequiredContinousDataFields.gen_1_kwhrs.value,
        F.when(F.col("gen_1_rh") == 0, 0).otherwise(
            F.col(RequiredContinousDataFields.gen_1_kwhrs.value)
        ),
    )

    df = df.withColumn(
        RequiredContinousDataFields.gen_2_kwhrs.value,
        F.when(F.col("gen_2_rh") == 0, 0).otherwise(
            F.col(RequiredContinousDataFields.gen_2_kwhrs.value)
        ),
    )

    df = df.withColumn(
        RequiredContinousDataFields.gen_3_kwhrs.value,
        F.when(F.col("gen_3_rh") == 0, 0).otherwise(
            F.col(RequiredContinousDataFields.gen_3_kwhrs.value)
        ),
    )

    df = df.withColumn(
        RequiredContinousDataFields.gen_4_kwhrs.value,
        F.when(F.col("gen_4_rh") == 0, 0).otherwise(
            F.col(RequiredContinousDataFields.gen_4_kwhrs.value)
        ),
    )

    # Recalculate TotalPower
    df = df.withColumn(
        "TotalPower",
        F.col(RequiredContinousDataFields.gen_1_kwhrs.value) +
        F.col(RequiredContinousDataFields.gen_2_kwhrs.value) +
        F.col(RequiredContinousDataFields.gen_3_kwhrs.value) +
        F.col(RequiredContinousDataFields.gen_4_kwhrs.value)
    )

    # Add Aux Engine constants and max loads
    for i in range(1, 5):
        df = df.withColumn(
            f"const_{i}",
            F.udf(lambda x: mcr_dict[x][f"AE{i}"]["const"])(
                F.col(RequiredContinousDataFields.shipname.value).cast(DoubleType())
            ),
        )

        df = df.withColumn(
            f"max_load_{i}",
            F.udf(lambda x: mcr_dict[x][f"AE{i}"]["max_load"])(
                F.col(RequiredContinousDataFields.shipname.value).cast(DoubleType())
            ),
        )

        df = df.withColumn(
            f"AE{i}_MCR", 
            F.col(f"const_{i}") * F.col(f"max_load_{i}")
        )

    # Calculate Load % for each auxiliary engine
    df = df.withColumn(
        "AE1_Load",
        F.col(RequiredContinousDataFields.gen_1_kwhrs.value) / F.col("max_load_1")
    )

    df = df.withColumn(
        "AE2_Load",
        F.col(RequiredContinousDataFields.gen_2_kwhrs.value) / F.col("max_load_2")
    )

    df = df.withColumn(
        "AE3_Load",
        F.col(RequiredContinousDataFields.gen_3_kwhrs.value) / F.col("max_load_3")
    )

    df = df.withColumn(
        "AE4_Load",
        F.col(RequiredContinousDataFields.gen_4_kwhrs.value) / F.col("max_load_4")
    )


    # Calculate Edemand
    df = df.withColumn(
        "Edemand",
        sum(
            (F.col(f"AE{i}_MCR") * F.col(f"AE{i}_Load") * F.col(f"gen_{i}_rh")) / eta
            for i in range(1, 5)
        ).cast(DoubleType()),
    )

    # Calculate denominator for AEU equation
    df = df.withColumn(
        "denom",
        sum(
            F.col(f"const_{i}") * F.col(f"AE{i}_MCR") * F.col(f"gen_{i}_rh")
            for i in range(1, 5)
        ).cast(DoubleType()),
    )

    df = df.withColumn(
        "aeu_lower_threshold",
        F.udf(lambda x: mcr_dict[x]["aeu_lower_threshold"])(
            F.col(RequiredContinousDataFields.shipname.value)
        ),
    )

    # Calculate GenRunning and EnergyTotal
    df = df.withColumn(
        "GenRunning",
        genrunning_udf(F.struct(*[f"gen_{i}_rh" for i in range(1, 5)])),
    )
    
    df = df.withColumn(
        "EnergyTotal",
        energytotal_udf(
            F.col("GenRunning"),
            F.col("Edemand"),
            F.col("max_load_1"),
            F.col("max_load_2"),
            F.col("max_load_3"),
            F.col("max_load_4"),
            F.col("const_1")
        )
    )

    # Calculate AEU
    df = df.withColumn(
        "AEU",
        F.when(
            (F.col("EnergyTotal") == 1) | (F.col("Edemand") / F.col("denom") >= 1), 1
        ).otherwise(F.round(F.col("Edemand") / F.col("denom"), 3)),
    )

    # Suponiendo que df es tu DataFrame
    ignore_conditions = [
        F.col(RequiredContinousDataFields.gen_1_kwhrs.value) > F.col("max_load_1"),
        F.col(RequiredContinousDataFields.gen_2_kwhrs.value) > F.col("max_load_2"),
        F.col(RequiredContinousDataFields.gen_3_kwhrs.value) > F.col("max_load_3"),
        F.col(RequiredContinousDataFields.gen_4_kwhrs.value) > F.col("max_load_4"),
        sum(F.col(f"gen_{i}_rh") for i in range(1, 5)) == 0,
        (F.col("TotalPower") == 0) & (sum(F.col(f"gen_{i}_rh") for i in range(1, 5)) > 0),
    ]

    # Puedes usar ignore_conditions para calcular una columna de bandera 'ignore_flag'
    df = df.withColumn("ignore", F.when(reduce(lambda x, y: x | y, ignore_conditions), 1).otherwise(0))

    # Calculate total capacity and ap_%
    df = df.withColumn("TOTALPOWER", F.col("TOTALPOWER").cast(FloatType()))
    df = df.withColumn("AVAILABLE POWER", F.col("AVAILABLE POWER").cast(FloatType()))
    df = df.withColumn("total_capacity", F.col("TOTALPOWER") + F.col("AVAILABLE POWER"))
    df = df.withColumn(
        "ap_%", F.round(F.col("AVAILABLE POWER") / F.col("total_capacity"), 3)
    )

    # Calculate correct_produced
    df = df.withColumn(
        "correct_produced",
        F.when((F.col("ap_%") <= 0.20) | (F.col("AEU") == 1), 1).otherwise(0),
    )

    # Calculate fuel savings
    df = df.withColumn(
        "fuel_flow", F.col(RequiredContinousDataFields.fuel_flow_cubic.value)
    )

    df = df.withColumn(
        "fuel_saving",
        (F.col("AEU") - F.col("aeu_lower_threshold")) * F.col("fuel_flow"),
    )

    df = df.withColumn(
        "best_case_fuel_saving", (1 - F.col("aeu_lower_threshold")) * F.col("fuel_flow")
    )

    # Filter out ignored rows
    df = df.filter(F.col("ignore") != 1)

    # Usar una expresión regular para extraer la hora del string
    extract_hour_udf = F.udf(lambda x: x.split(":")[1])

    df = df.withColumn('Hour', extract_hour_udf(F.col(RequiredContinousDataFields.time.value)))
    df = df.withColumn('REPORT_DATE', F.to_timestamp(F.col(RequiredContinousDataFields.date.value)))
    print(df.columns)
    df = df.dropna(how="any", subset="Hour")
    df.display()
    # Crear la lista de índices
    indexs = ["SHIPNAME", "REPORT_DATE", "CPT_DOC_NO", "CPT_FULLNAME", "CPT_ID", "CHENG_DOC_NO", "CHENG_FULLNAME", "CHENG_ID",  "aeu_lower_threshold"]

    hours = [row["Hour"] for row in df.select("Hour").distinct().collect()]

    pivot_df = df.groupBy(*indexs).pivot("Hour", hours).agg(
        F.first("AEU").alias("AEU"),
        F.first("correct_produced").alias("correct_produced"),
        F.first("fuel_saving").alias("fuel_saving"),
        F.first("best_case_fuel_saving").alias("best_case_fuel_saving")
    )

    # Renombrar las columnas con el formato deseado
    for col in ["AEU", "correct_produced", "fuel_saving", "best_case_fuel_saving"]:
        for hour in hours:
            pivot_df = pivot_df.withColumnRenamed(f"{hour}_{col}", f"{col}_{hour}")

    # Obtener los nombres exactos de las columnas generadas después del pivot
    generated_columns = pivot_df.columns

    # Calcular total_hours, total_good_hours, y overproduced_hours
    total_hours_expr = reduce(lambda a, b: a + b, (F.when(F.col(col).isNotNull(), 1)
                                                   .otherwise(0) for col in generated_columns if col.startswith("correct_produced_")))

    total_good_hours_expr = reduce(lambda a, b: a + b, (F.coalesce(F.col(col), F.lit(0)) for col in generated_columns if col.startswith("correct_produced_")))

    overproduced_hours_expr = F.col("total_hours") - F.col("total_good_hours")

    pivot_df = pivot_df.withColumn("total_hours", total_hours_expr)
    pivot_df = pivot_df.withColumn("total_good_hours", total_good_hours_expr)
    pivot_df = pivot_df.withColumn("overproduced_hours", overproduced_hours_expr)

    # Eliminar filas con menos de 10 horas de datos
    pivot_df = pivot_df.filter(F.col("total_hours") >= 10)

    # Calcular epm_success
    pivot_df = pivot_df.withColumn(
        "epm_success",
        F.when(F.col("overproduced_hours") <= 3, 1).otherwise(0)
    )

    # Calcular epm_fuel_saved y epm_best_fuel_saved
    fuel_saving_expr = sum(F.coalesce(F.col(col), F.lit(0)) for col in generated_columns if col.startswith("fuel_saving_"))
    best_case_fuel_saving_expr = sum(F.coalesce(F.col(col), F.lit(0)) for col in generated_columns if col.startswith("best_case_fuel_saving_"))

    pivot_df = pivot_df.withColumn("epm_fuel_saved", fuel_saving_expr)
    pivot_df = pivot_df.withColumn("epm_best_fuel_saved", best_case_fuel_saving_expr)
   
    # Drop columns that end with _{time}
    columns_to_drop = [
        col for col in pivot_df.columns if col.split("_")[-1].isdigit()
    ]
    pivot_df = pivot_df.drop(*columns_to_drop)
    
    # Combine pivot_df and nonIOT
    nonIOT = nonIOT.withColumn("total_hours", F.lit(0))
    nonIOT = nonIOT.withColumn("total_good_hours", F.lit(0))
    nonIOT = nonIOT.withColumn("aeu_lower_threshold", F.lit(0))
    nonIOT = nonIOT.withColumn("overproduced_hours", F.lit(0))
    nonIOT = nonIOT.withColumn("epm_success", F.lit(0))
    nonIOT = nonIOT.withColumn("epm_fuel_saved", F.lit(0))
    nonIOT = nonIOT.withColumn("epm_best_fuel_saved", F.lit(0))

    output_df = pivot_df.union(nonIOT.select(pivot_df.columns))

    return output_df

def process_noon_data(data: DataFrame) -> DataFrame:

    df = data.select([F.col(c).alias(c.upper()) for c in data.columns])

    
    window = Window.partitionBy('DIMSHIPID').orderBy('REPORTDATETIMEGMT', 'REPORTTYPE')

    df = df.withColumn('VOYAGELEGFROMPORTNAME', F.when(F.col('VOYAGELEGFROMPORTNAME') == '\\N', None).otherwise(F.col('VOYAGELEGFROMPORTNAME')))
    df = df.withColumn('VOYAGELEGTOPORTNAME', F.when(F.col('VOYAGELEGTOPORTNAME') == '\\N', None).otherwise(F.col('VOYAGELEGTOPORTNAME')))
    df = df.withColumn('NEWVOYAGELEGFROMPORTNAME', F.last('VOYAGELEGTOPORTNAME', ignorenulls=True).over(window))
    df = df.withColumn('VOYAGELEGFROMPORTNAME', F.when(F.col('VOYAGELEGTOPORTNAME').isNull(), F.col('NEWVOYAGELEGFROMPORTNAME')).otherwise(F.col('VOYAGELEGFROMPORTNAME')))
    df = df.withColumn('VOYAGELEGTOPORTNAME', F.when(F.col('VOYAGELEGTOPORTNAME').isNull(), F.col('NEWVOYAGELEGFROMPORTNAME')).otherwise(F.col('VOYAGELEGTOPORTNAME')))
    
    df = df.drop('NEWVOYAGELEGFROMPORTNAME')

    # Keep only noon reports type N or A, keep the latest report for each day
    df = df.filter(F.col('REPORTTYPE').isin(['N', 'A']))
    window = Window.partitionBy('DIMSHIPID', 'DIMREPORTDATEID').orderBy(F.desc('REPORTDATETIMEGMT'), F.desc('REPORTTYPE'))
    df = df.withColumn('row_number', F.row_number().over(window)).filter(F.col('row_number') == 1).drop('row_number')

    return df, "raw"

def process_fuel_data(data: DataFrame) -> DataFrame:
    # Convert the date column into date format
    data = data.withColumn('REPORT_DATE', F.to_date(F.col('ReportDateTimeGMT')))

    data = data.filter(F.col('ShipName') != 'Stolt Ocelot')

    # Create a pivot table
    pivot_df = data.groupBy('ShipName', 'REPORT_DATE').pivot('FuelType').agg(
        F.sum('PropulsionConsumption').alias('PropulsionConsumption'),
        F.sum('GeneratorConsumption').alias('GeneratorConsumption')
    )

    # Calculate total consumptions
    fuel_types = data.select('FuelType').distinct().rdd.flatMap(lambda x: x).collect()
    pivot_df = pivot_df.withColumn('TotalGeneratorConsumptionNEW', 
        sum([F.col(f'{fuel_type}_GeneratorConsumption') for fuel_type in fuel_types])
    )
    pivot_df = pivot_df.withColumn('TotalPropulsionConsumptionNEW', 
        sum([F.col(f'{fuel_type}_PropulsionConsumption') for fuel_type in fuel_types])
    )

    # Reorder columns
    columns = pivot_df.columns
    column_order = ['ShipName', 'REPORT_DATE', 'TotalGeneratorConsumptionNEW', 'TotalPropulsionConsumptionNEW'] + [col for col in columns if col not in ['ShipName', 'REPORT_DATE', 'TotalGeneratorConsumptionNEW', 'TotalPropulsionConsumptionNEW']]

    result_df = pivot_df.select(column_order)

    return result_df
