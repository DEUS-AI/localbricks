# move this data vault model to general aviation folder
tables_schemas = {
    "LINK_Flight_Crew": [
        "event_id",
        "event_openid",
        "customer_code",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "LINK_Flight_IOT": [
        "event_id",
        "event_openid",
        "customer_code",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "SAT_IOT_Records": [
        "event_id",
        "selected_altitude_mt",
        "avg_altitude_mt_cruise",
        "cd_behaviour_success",
        "taxi_out_minutes",
        "avg_speed_knots_cruise",
        "total_approach_fuel_burned",
        "total_dfa_fuel_burned",
        "origin",
        "destination",
        "fuel_burn_taxi_out1",
        "fuel_burn_taxi_in1",
        "fuel_burn_taxi_out2",
        "departure_rwy",
        "plnd_block_fuel",
        "arrival_rwy",
        "role_alias",
        "operator_id",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "SAT_Flight_IOT_Stats": [
        "event_id",
        "selected_altitude_mt",
        "avg_altitude_mt_cruise",
        "avg_speed_knots_cruise",
        "air_minutes",
        "ground_minutes",
        "act_bloff_fuel",
        "takeoff_fuel",
        "cruise_start_time",
        "cruise_end_time",
        "landing_fuel",
        "gate_fuel",
        "taxi_out_minutes",
        "cruise_minutes",
        "descent_fuel",
        "taxi_in_minutes",
        "plnd_tanker_fuel",
        "total_engine1_runtime_mins",
        "total_engine2_runtime_mins",
        "fuel_burn_total1",
        "fuel_burn_total2",
        "fuel_burn_taxi_out1",
        "fuel_burn_taxi_out2",
        "fuel_burn_taxi_in1",
        "fuel_burn_taxi_in2",
        "flaps_deployed_altitude",
        "flaps_deployed_distance",
        "flaps_deployed_speed",
        "tom_trip_coefficient_x3",
        "tom_trip_coefficient_x2",
        "tom_trip_coefficient_x",
        "tom_trip_coefficient_intercept",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "HUB_Crew_Member": [
        "operator_id",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "SAT_Crew_Member": [
        "operator_uuid",
        "operator_code_1",
        "operator_secondary_code",
        "operator_name",
        "operator_lastname",
        "role_alias",
        "team_id",
        "team_name",
        "vehicle_email_operator1",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "HUB_Flight": [
        "event_id",
        "ocurrence_datetime",
        "file_sources",
        "event_deus_id"
    ],
    "SAT_Flight_Planned": [
        "event_deus_id",
        "event_openid",
        "haul_type",
        "vehicle",
        "flight_number_iata",
        "flight_number_icao",
        "dds_code",
        "origin",
        "departure_rwy",
        "destination",
        "arrival_rwy",
        "route_distance",
        "planned_zfw",
        "total_pax_qty",
        "plnd_arrival_time",
        "ocurrence_datetime",
        "plnd_departure_time"
    ],
    "SAT_Fuel_Planned": [
        "event_deus_id",
        "plnd_block_fuel",
        "planned_extra_fuel",
        "planned_discretionary_fuel",
        "planned_contingency_fuel",
        "planned_alternate_fuel",
        "planned_final_reserve_fuel",
        "planned_tanker_fuel",
        "planned_taxi_fuel",
        "fuel_burn_reto_1_without_warmup",
        "planned_trip_fuel",
        "planned_takeoff_fuel",
        "planned_arrival_fuel",
        "planned_landing_fuel",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "SAT_Fuel_Actual": [
        "event_deus_id",
        "fuel_burn_reti_main_engine",
        "planned_extra_fuel",
        "planned_discretionary_fuel",
        "planned_contingency_fuel",
        "planned_alternate_fuel",
        "planned_final_reserve_fuel",
        "planned_tanker_fuel",
        "fuel_burn_reto_main_engine",
        "planned_takeoff_fuel",
        "planned_arrival_fuel",
        "planned_landing_fuel",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "SAT_Flight_Actual": [
        "event_deus_id",
        "event_openid",
        "vehicle",
        "flight_number_iata",
        "flight_number_icao",
        "dds_code",
        "origin",
        "departure_rwy",
        "plnd_departure_time",
        "destination",
        "arrival_rwy",
        "plnd_arrival_time",
        "route_distance",
        "actual_zfw",
        "total_pax_qty",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "HUB_Time": [
        "event_id",
        "plnd_departure_time",
        "ocurrence_datetime",
        "source_sourcefilename"
    ],
    "SAT_Behaviours": [
        "event_deus_id",
        "event_openid",
        "reti__net_savings",
        "reti__success",
        "reti__savings",
        "reti__best_case_savings",
        "reti__reason",
        "reto__net_savings",
        "reto__success",
        "reto__savings",
        "reto__best_case_savings",
        "reto__reason",
        "reto__other_reasons",
        "ef__net_savings",
        "ef__success",
        "ef__savings",
        "ef__best_case_savings",
        "ef__reason",
        "odfl__net_savings",
        "odfl__success",
        "odfl__savings",
        "odfl__best_case_savings",
        "odfl__reason"
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
    
