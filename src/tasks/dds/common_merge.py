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

def load_mapping_file(dds_code: str):        
    config = json.loads(get_dbutils().fs.head(f"dbfs:/FileStore/{dds_code.lower()}/mapping.json"))
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

def collect_data_fields(dds_code: str):
    mapping = load_mapping_file(dds_code)
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

def generate_deus_open_id(df: DataFrame, join_keys: list, dds_code: str) -> DataFrame:
    concat_expr = F.concat_ws('_', *[F.col(key) for key in join_keys])    
    
    df = df.drop("deus_open_id")
    df = df.withColumn('deus_open_id', F.concat(F.lit(f"{dds_code.upper()}_"), concat_expr))
    
    return df

def add_default_columns_spark(df):
    mapping = load_mapping_file("vs")  # TODO: Make this configurable
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

def apply_mapping_for_dds_code(dds_code: str, df):
    mapping = load_mapping_file(dds_code)
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
                        #Operator_id replaced with "" when null, this should be moved into gx probably
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

    # Add domain and DDS code
    df = df.withColumn('event_id', F.lit("Flight"))
    df = df.withColumn('dds_code', F.lit(dds_code))
    df = df.withColumn('upload_date', F.lit(datetime.date.today().isoformat()))
    
    return df   

def ensure_all_fields(df: DataFrame) -> DataFrame:
    mappings = load_mapping_file("vs")  # TODO: Make this configurable

    for field in mappings['dataMappings']['csvFields']:
        field_name = field['name']
        field_type = field['type']
        default_value = field.get('default_value')
        
        if field_name not in df.columns:
            print(f"Processing field '{field_name}' not in DataFrame")
            if default_value is not None:
                df = df.withColumn(field_name, F.lit(default_value).cast(TYPE_MAP[field_type]))
            else:                
                df = df.withColumn(field_name, F.lit(None).cast(TYPE_MAP[field_type]))
    
    return df

def apply_formulas(df, dds_code: str):
    import_dds_module(dds_code=dds_code, module_name='formulas')
    
    mapping = load_mapping_file(dds_code)
    
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

def import_dds_module(dds_code: str, module_name: str):
    """Import a module from the DDS code's directory.
    
    Args:
        dds_code (str): The DDS code to import from
        module_name (str): The name of the module to import
    """
    try:
        module = importlib.import_module(f"src.tasks.dds.{dds_code.lower()}.{module_name}")
        for attr in dir(module):
            if not attr.startswith('_'):
                globals()[attr] = getattr(module, attr)
    except ImportError as e:
        logger.warning(f"Could not import {module_name} for DDS code {dds_code}: {str(e)}")
        pass

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



