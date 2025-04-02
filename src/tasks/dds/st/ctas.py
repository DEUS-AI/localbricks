tables_schemas = {
    "LINK_Ship_Daily_Crew": [
        "hk_ship_day_id",
        "hk_operator_id",
        "load_date",
        "record_source"
    ],
    "HUB_Crew_Member": [
        "hk_operator_id",
        "load_date",
        "record_source"
    ],
    "SAT_Crew_Member": [
        "hk_operator_id",
        "operator_code",
        "operator_license",
        "operator_name",
        "operator_email",
        "operator_role",
        "load_date",
        "record_source"
    ],
    "HUB_Ship_Day": [
        "hk_ship_day_id",
        "ship_imo",
        "sea_date",
        "load_date",
        "record_source"
    ],
    "SAT_Voyage_Details": [
        "hk_ship_day_id",
        "ship_name",
        "ship_type",
        "ship_engine_type",
        "voyage_type",
        "voyage_number",
        "departure_port",
        "arrival_port",
        "departure_time",
        "arrival_time",
        "voyage_duration_days",
        "load_date",
        "record_source"
    ],
    "SAT_Daily_Stats": [
        "hk_ship_day_id",
        "voyage_leg_id",
        "terminal",
        "position",
        "day",
        "origin_port",
        "next_port",
        "status",
        "log_hours",
        "steaming_hours",
        "manouvering_hours",
        "port_hours",
        "sea_hours",
        "speed_water",
        "observed_distance",
        "main_engine_1_running_hours",
        "main_engine_2_running_hours",
        "main_engine_3_running_hours",
        "main_engine_4_running_hours",
        "main_engine_1_electric_power",
        "main_engine_2_electric_power",
        "main_engine_3_electric_power",
        "main_engine_4_electric_power",
        "aux_engine_1_running_hours",
        "aux_engine_2_running_hours",
        "aux_engine_3_running_hours",
        "aux_engine_4_running_hours",
        "aux_engine_5_running_hours",
        "aux_engine_6_running_hours",
        "aux_engine_1_electric_power",
        "aux_engine_2_electric_power",
        "aux_engine_3_electric_power",
        "aux_engine_4_electric_power",
        "aux_engine_5_electric_power",
        "aux_engine_6_electric_power",
        "main_engine_sfoc",
        "aux_engine_sfoc",
        "aft_draft",
        "fwd_draft",
        "draft_type",
        "wind_speed",
        "wind_direction",
        "swell_height",
        "sea_height",
        "sea_temperature",
        "beaufort",
        "trim",
        "more_tbd",
        "more_tbd",
        "more_tbd",
        "more_tbd",
        "more_tbd",
        "more_tbd",
        "load_date",
        "record_source"
    ],
    "SAT_Report_Records": [
        "hk_ship_day_id",
        "report_source_id",
        "report_time_utc",
        "report_type",
        "operational_mode",
        "log_hours",
        "latitude",
        "longitude",
        "remarks",
        "report_data_json",
        "more_tbd",
        "more_tbd",
        "more_tbd",
        "load_date",
        "record_source"
    ],
    "SAT_Logbook_Records": [
        "hk_ship_day_id",
        "logbook_source_id",
        "logbook_reported_time_utc",
        "logbook_reported_by",
        "logbook_book",
        "logbook_category",
        "logbook_summary",
        "logbook_field_name",
        "logbook_field_value",
        "logbook_field_unit",
        "load_date",
        "record_source"
    ],
    "SAT_Constant_Params": [
        "hk_ship_day_id",
        "param_name",
        "param_value",
        "param_description",
        "load_date",
        "record_source"
    ]
}

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

def remove_unexisting_columns(merged_data: DataFrame) -> dict:
    merged_columns = set(merged_data.columns)
    
    updated_schemas = {}
    
    for table, schema in tables_schemas.items():
        updated_schema = [col for col in schema if col in merged_columns]
        updated_schemas[table] = updated_schema
    
    return updated_schemas

def generate_ctas(df, table_name, reference_schema):
    select_expr = [
        (col(col_name) if col_name in df.columns else lit(None).alias(col_name))
        for col_name in reference_schema
    ]

    df_selected = df.select(*select_expr)
    df_selected.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name) 
    
