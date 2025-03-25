import re
import sys, os
pattern = r'^(.*?/files)'
match = re.search(pattern, os.path.abspath(__name__))
sys.path.append(match.group(1))

import logging
from deus_lib.utils import sparksession
import datetime
import json
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace, to_timestamp, date_format
from pyspark.sql.types import TimestampType, DecimalType
from deus_lib.utils.common import get_dbutils
import importlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TYPE_MAP = {
    "string": "string",
    "date":   "timestamp",
    "number": "decimal(18,5)"
}

def load_mapping_file(cc):        
    config = json.loads(get_dbutils().fs.head(f"dbfs:/FileStore/{cc.lower()}/mapping.json"))
    return config

def clean_column_name(col_name):
    col_name = col_name.strip()
    col_name = col_name.lower()
    col_name = re.sub(r'\s+', ' ', col_name)
    col_name = re.sub(r'\s', '_', col_name)
    col_name = re.sub(r'[^a-zA-Z0-9_]', '', col_name)
    return col_name

def clean_df_header(df):
    original_column_names = df.columns
    cleaned_column_names = [clean_column_name(col) for col in original_column_names]
    for original, cleaned in zip(original_column_names, cleaned_column_names):
        df = df.withColumnRenamed(original, cleaned)
        
    return df    

def collect_data_fields(customer):
    mapping = load_mapping_file(customer)
    data_fields = []
    for field in mapping['dataMappings']['csvFields']:
        field_name = field['name']
        data_fields.append(field_name)
        
    return data_fields

def parse_dates(df, date_field):
    # Convert to timestamp with multiple formats
    date_formats = [
        "yyyy-MM-dd HH:mm:ss.SSSSSSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        "dd/MM/yyyy HH:mm:ss"
    ]
    
    for formatter in date_formats:
        df = df.withColumn(
            date_field,
            when(
                col(date_field).isNotNull() & col(date_field).cast(TimestampType()).isNull(),
                to_timestamp(col(date_field), formatter)
            ).otherwise(col(date_field))
        )
    
    df = df.withColumn(date_field, date_format(col(date_field).cast(TimestampType()), "yyyy-MM-dd HH:mm:ss"))

    return df


def remove_spaces(df: DataFrame) -> DataFrame:
    renamed_cols = [col(c).alias(c.replace(' ', '_')) if ' ' in c else col(c) for c in df.columns]
    return df.select(*renamed_cols)


def generate_deus_open_id(df: DataFrame, join_keys: list, cc: str) -> DataFrame:
    concat_expr = F.concat_ws('_', *[F.col(key) for key in join_keys])    
    
    df = df.drop("deus_open_id")
    df = df.withColumn('deus_open_id', F.concat(F.lit(f"{cc.upper()}_"), concat_expr))
    
    return df

def add_default_columns_spark(df):
    mapping = load_mapping_file("vs")
    for field in mapping['dataMappings']['csvFields']:
        if 'options' in field and isinstance(field['options'], list) and len(field['options']) == 0:
            column_name = field['name']
            colum_type = field['type']
            default_value = field.get('default_value', None)
            if column_name not in df.columns:
                if default_value:
                    df = df.withColumn(column_name, F.lit(default_value).cast(TYPE_MAP[colum_type]))
                else:
                    df = df.withColumn(column_name, F.lit(None).cast(TYPE_MAP[colum_type]))
    return df    

def apply_mapping_for_customer(customer, df):
    mapping = load_mapping_file(customer) #no need to do this again
    date_formats = mapping.get('dateFormat', [])

    for field in mapping['dataMappings']['csvFields']:
        print(f"Applying mapping for field: {field['name']}")
        field_name = field['name']
        field_type = field['type']
        field_options = field.get('options', [])    
        default_value = field.get('default_value', None)
        field_regexp = field.get('regexp', None)
        field_formula = field.get('formula', None)
        field_options.append(field_name)

        for option in field_options: 
            if option in df.columns:
                df = df.withColumnRenamed(option, field_name)
                
                if field_regexp:
                    df = df.withColumn(field_name, F.when(
                        F.col(field_name).rlike(field_regexp), F.col(field_name).cast(TYPE_MAP[field_type])
                    ).otherwise(
                        F.lit(default_value).cast(TYPE_MAP[field_type])
                    ))   
                
                if field_type == 'number':
                    df = df.withColumn(field_name, col(field_name).cast(TYPE_MAP[field_type]))

                if field_type == 'boolean':
                    df = df.withColumn(field_name, col(field_name).cast("boolean"))
                
                if field_type == 'string':
                    if 'operator_id' in field_name:
                        #Operator_id replaced with “” when null, this should be moved into gx probably
                        df = df.withColumn(field_name, when(col(field_name).isNull(), "").otherwise(col(field_name)))
                        df = df.withColumn(field_name, regexp_replace(col(field_name).cast("string"), r'\s+', ''))
                        df = df.withColumn(field_name, regexp_replace(col(field_name), r'^0+', ''))
                    else:
                        df = df.withColumn(field_name, col(field_name).cast("string"))                
                
                if field_type == 'date':
                    df = parse_dates(df, field_name)

                break
            else:
                print(f"Field {option} not found in DataFrame for {field_name}")

    # TODO Apply industry
    df = df.withColumn('event_id', F.lit("Flight"))
    df = df.withColumn('customer_code', F.lit(customer))
    df = df.withColumn('upload_date', F.lit(datetime.date.today().isoformat()))
    
    return df   

def ensure_all_fields(df: DataFrame) -> DataFrame:
    mappings = load_mapping_file("vs")

    for field in mappings['dataMappings']['csvFields']:
        field_name = field['name']
        field_type = field['type']
        default_value = field.get('default_value')
        
        if field_name not in df.columns:
            print(f"Processing field '{field_name}' no in DataFrame")
            if default_value is not None:
                df = df.withColumn(field_name, F.lit(default_value).cast(TYPE_MAP[field_type]))
            else:                
                df = df.withColumn(field_name, F.lit(None).cast(TYPE_MAP[field_type]))
    
    return df

def apply_formulas(df, cc):
    import_client_module(client_code=cc, module_name='formulas')
    
    mapping = load_mapping_file(cc)
    
    for field in mapping['dataMappings']['csvFields']:
        field_name = field['name']
        field_type = field['type']
        if 'formula' in field:
            formula_name = f"calculate_{field_name}"
            state = field["state"]
            print(f"Applying formula {formula_name} for field {field_name} of type {TYPE_MAP[field_type]}")
            df = globals()[formula_name](df, state, TYPE_MAP[field_type])
        else:
            pass

    return df

def convert_double_to_decimal(df, precision=18, scale=5):
    for column, dtype in df.dtypes:
        if dtype == "double" or dtype == "decimal":
            df = df.withColumn(column, df[column].cast(DecimalType(precision, scale)))
    return df


def clean_merged_data(self, data: DataFrame, expected_columns: list) -> DataFrame:
    
    output_data = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=expected_columns)
    
    if data.count() > 0:
        for col in expected_columns:
            original = col
            left = f"{col}_x"  # 1st dataframe
            right = f"{col}_y" # 2nd dataframe

            # Añadir la columna original si no existe en los datos
            if original not in data.columns:
                data = data.withColumn(original, F.lit(None))

            # Mantener los valores antiguos
            if left in data.columns:
                output_data = output_data.withColumn(original, F.col(left))
            else:
                output_data = output_data.withColumn(original, F.col(original))

            # Actualizar si los nuevos valores no son NA
            if right in data.columns:
                output_data = output_data.withColumn(original, F.when(F.col(right).isNotNull(), F.col(right)).otherwise(F.col(original)))
    
    return output_data

def import_client_module(client_code: str, module_name: str):
    try:
        client_base_dir = f'src.tasks.clients.{client_code}'
        module = importlib.import_module(f'{client_base_dir}.{module_name}'.lower())
        logger.info(f"Module {module_name} imported successfully.")
        globals().update({ name: getattr(module, name) for name in dir(module) if not name.startswith('_') })
        return module
    except ModuleNotFoundError:
        logger.info(f"Module {module_name} not found.")
        return None
    

def mariapps_merge(sdfs: dict[DataFrame]):

    # Prepare crew_data_df
    crew_data_df = sdfs['crew_data'].withColumnRenamed('source_file_name', 'crew_source_file_name') \
                                    .drop("event_id", "file_path", "ingestion_datetime", "ingestion_job_run_id", "status","upload_date")

    # Prepare mariapps_df
    mariapps_df = sdfs['mariapps'].drop("event_id", "file_path", "ingestion_datetime", "ingestion_job_run_id", "status","upload_date")

    # Identify intersecting and distinct columns
    mariapps_columns = set(mariapps_df.columns)
    crew_columns = set(crew_data_df.columns)
    intersect_columns = set(crew_data_df.columns).intersection(set(mariapps_df.columns))

    result_df = mariapps_df.alias("ms").join(
        crew_data_df.alias("cw1"),
        (F.col("ms.operator_id") == F.col("cw1.operator_id")),
        "left"  # Changed to left join to preserve all mariapps data
    ).join(
        crew_data_df.alias("cw2"),
        (F.col("ms.operator_id2") == F.col("cw2.operator_id")),
        "inner"
    ).filter(
        F.to_date(F.col("ms.report_date_utc")) < F.current_date()
    )

    # Final column selection
    select_expr = (
        [F.col("ms." + c).alias(c) for c in mariapps_columns] +
        [F.col("cw1." + c).alias(c) for c in crew_columns if c not in intersect_columns] +
        [F.col("cw1.discharge_book_number").alias("operator_secondary_code"),
         F.col("cw2.discharge_book_number").alias("operator_secondary_code2"),
         F.when(F.col("cw1.operator_id").isNotNull(), F.lit("Captain")).otherwise(F.lit(None)).alias("role_alias"),
         F.when(F.col("cw2.operator_id").isNotNull(), F.lit("Chief Engineer")).otherwise(F.lit(None)).alias("role_alias2")]
    )

    result_df = result_df.select(*select_expr).drop("discharge_book_number")
    
    return result_df



