from pyspark.sql.types import DecimalType
from pyspark.sql import functions as F
from pyspark.sql import Window



def calculate_event_id(df, state, field_type):
    return df.withColumn("event_id", F.expr("uuid()"))


def calculate_fuel_burn_reti_main_engine(df, state, field_type):
    return df.withColumn(
        "fuel_burn_reti_main_engine",
        F.greatest(
            F.col("fuel_burn_taxi_in1_kg"),
            F.col("fuel_burn_taxi_in2_kg"),
            F.col("fuel_burn_taxi_in3_kg"),
            F.col("fuel_burn_taxi_in4_kg"),
        ),
    )



def calculate_fuel_burn_reto_main_engine(df, state, field_type):
    return df.withColumn(
        "fuel_burn_reto_main_engine",
        F.greatest(
            F.col("fuel_burn_taxi_out1_kg"),
            F.col("fuel_burn_taxi_out2_kg"),
            F.col("fuel_burn_taxi_out3_kg"),
            F.col("fuel_burn_taxi_out4_kg"),
        ),
    )

def calculate_total_expected_fuel(df, state, field_type):
    return df.withColumn("total_expected_fuel", F.col("plnd_block_fuel") * 1.005)


def calculate_projected_trip_fuel(df, state, field_type):
    return df.withColumn("projected_trip_fuel", F.col("plnd_trip_fuel") * 1.005)


def calculate_act_takeoff_fuel(df, state, field_type):

    return df.withColumn(
        "act_takeoff_fuel",
        F.when(
            (F.col("act_bloff_fuel").isNotNull())
            & (F.col("act_taxi_out_fuel").isNotNull())
            & (F.col("act_taxi_out_fuel") >= 0),
            F.col("act_bloff_fuel") - F.col("act_taxi_out_fuel"),
        ).otherwise(F.col("act_takeoff_fuel")),
    )


def calculate_act_landing_fuel(df, state, field_type):
    return df.withColumn(
        "act_landing_fuel",
        F.when(
            (F.col("act_blon_fuel").isNotNull())
            & (F.col("act_taxi_in_fuel").isNotNull())
            & (F.col("act_taxi_in_fuel") >= 0),
            F.col("act_blon_fuel") + F.col("act_taxi_in_fuel"),
        ).otherwise(F.col("act_landing_fuel")),
    )


def calculate_act_trip_fuel(df, state, field_type):
    return df.withColumn(
        "act_trip_fuel", F.col("act_takeoff_fuel") - F.col("act_landing_fuel")
    )


def calculate_plnd_flight_time_hs(df, state, field_type):
    return df.withColumn(
        "plnd_flight_time_hs",
        F.when(
            F.col("plnd_flight_time").isNotNull(), F.col("plnd_flight_time") / 60
        ).otherwise(F.col("plnd_flight_time_secs") / 60 / 60),
    )


def calculate_actual_flight_time_hs(df, state, field_type):
    return df.withColumn(
        "actual_flight_time_hs",
        F.when(
            F.col("actual_flight_time").isNotNull(), F.col("actual_flight_time") / 60
        ).otherwise(
            (
                F.col("cruise_duration")
                + F.col("approach_duration")
                + F.col("landing_duration")
                + F.col("toc_duration")
                + F.col("tod_duration")
            )
            / 60
        ),
    )


def calculate_plnd_airborne_time(df, state, field_type):
    return df.withColumn(
        "plnd_airborne_time",
        F.when(
            F.col("plnd_flight_time").isNotNull(), F.col("plnd_flight_time")
        ).otherwise(
            (
                (
                    F.unix_timestamp("plnd_arrival_time")
                    - F.unix_timestamp("plnd_departure_time")
                )
                / 60
                - F.col("planned_taxi_time")
            )
            / 60
        ),
    )


def calculate_airborne_time(df, state, field_type):
    return df.withColumn(
        "airborne_time",
        F.when(
            F.col("actual_flight_time_hs").isNotNull(), F.col("actual_flight_time_hs")
        ).otherwise(
            (
                (
                    F.unix_timestamp("arrival_datetime")
                    - F.unix_timestamp("ocurrence_datetime")
                )
                / 60
                - F.col("taxi_in_minutes")
                - F.col("taxi_out_minutes")
            )
            / 60
        ),
    )


def calculate_duration(df, state, field_type):
    return df.withColumn(
        "duration",
        F.when(F.col("airborne_time") <= 3, "SHORT")
        .when(F.col("airborne_time") <= 6, "MID")
        .otherwise("LONG"),
    )


def calculate_weekday(df, state, field_type):
    return df.withColumn("weekday", F.dayofweek("ocurrence_datetime") - 1)


def calculate_team_name(df, state, field_type):
    return df.withColumn(
        "team_name",
        F.when(F.col("vehicle").startswith("B78"), "VS B787 Team").otherwise(
            "VSDefaultTeam"
        ),
    )


def calculate_vehicle(df, state, field_type):
    conditions = [
        (F.col("aircraft_reg_code").isin(reg_codes), F.lit(aircraft))
        for aircraft, reg_codes in state.items()
    ]
    df = df.withColumn(
        "vehicle", F.coalesce(*[F.when(cond, val) for cond, val in conditions])
    )
    return df


def calculate_act_zero_fuel_weight_kg(df, state, field_type):
    df = df.withColumn(
        "act_zero_fuel_weight_kg",
        F.when(
            F.col("vehicle") == "B787", F.col("act_zero_fuel_weight") * 0.454
        ).otherwise(F.col("act_zero_fuel_weight")),
    )
    return df


def calculate_fuel_burn_climb1_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_climb1_kg",
        F.when(F.col("vehicle") == "B787", F.col("fuel_burn_climb1") * 0.454).otherwise(
            F.col("fuel_burn_climb1")
        ),
    )
    return df


def calculate_fuel_burn_climb2_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_climb2_kg",
        F.when(F.col("vehicle") == "B787", F.col("fuel_burn_climb2") * 0.454).otherwise(
            F.col("fuel_burn_climb2")
        ),
    )
    return df


def calculate_fuel_burn_climb3_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_climb3_kg",
        F.when(F.col("vehicle") == "B787", F.col("fuel_burn_climb3") * 0.454).otherwise(
            F.col("fuel_burn_climb3")
        ),
    )
    return df


def calculate_fuel_burn_climb4_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_climb4_kg",
        F.when(F.col("vehicle") == "B787", F.col("fuel_burn_climb4") * 0.454).otherwise(
            F.col("fuel_burn_climb4")
        ),
    )
    return df


def calculate_fuel_burn_cruise1_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_cruise1_kg",
        F.when(
            F.col("vehicle") == "B787", F.col("fuel_burn_cruise1_kg") * 0.454
        ).otherwise(F.col("fuel_burn_cruise1_kg")),
    )
    return df


def calculate_fuel_burn_cruise2_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_cruise2_kg",
        F.when(
            F.col("vehicle") == "B787", F.col("fuel_burn_cruise2_kg") * 0.454
        ).otherwise(F.col("fuel_burn_cruise2_kg")),
    )
    return df


def calculate_fuel_burn_cruise3_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_cruise3_kg",
        F.when(
            F.col("vehicle") == "B787", F.col("fuel_burn_cruise3_kg") * 0.454
        ).otherwise(F.col("fuel_burn_cruise3_kg")),
    )
    return df


def calculate_fuel_burn_cruise4_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_cruise4_kg",
        F.when(
            F.col("vehicle") == "B787", F.col("fuel_burn_cruise4_kg") * 0.454
        ).otherwise(F.col("fuel_burn_cruise4_kg")),
    )
    return df


def calculate_fuel_burn_descent1_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_descent1_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_descent1"] * 0.454).otherwise(
            df["fuel_burn_descent1"]
        ),
    )
    return df


def calculate_fuel_burn_descent2_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_descent2_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_descent2"] * 0.454).otherwise(
            df["fuel_burn_descent2"]
        ),
    )
    return df


def calculate_fuel_burn_descent3_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_descent3_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_descent3"] * 0.454).otherwise(
            df["fuel_burn_descent3"]
        ),
    )
    return df


def calculate_fuel_burn_descent4_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_descent4_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_descent4"] * 0.454).otherwise(
            df["fuel_burn_descent4"]
        ),
    )
    return df


def calculate_fuel_burn_approach1_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_approach1_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_approach1"] * 0.454).otherwise(
            df["fuel_burn_approach1"]
        ),
    )
    return df


def calculate_fuel_burn_approach2_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_approach2_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_approach2"] * 0.454).otherwise(
            df["fuel_burn_approach2"]
        ),
    )
    return df


def calculate_fuel_burn_approach3_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_approach3_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_approach3"] * 0.454).otherwise(
            df["fuel_burn_approach3"]
        ),
    )
    return df


def calculate_fuel_burn_approach4_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_approach4_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_approach4"] * 0.454).otherwise(
            df["fuel_burn_approach4"]
        ),
    )
    return df


def calculate_fuel_burn_landing1_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_landing1_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_landing1"] * 0.454).otherwise(
            df["fuel_burn_landing1"]
        ),
    )
    return df


def calculate_fuel_burn_landing2_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_landing2_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_landing2"] * 0.454).otherwise(
            df["fuel_burn_landing2"]
        ),
    )
    return df


def calculate_fuel_burn_landing3_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_landing3_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_landing3"] * 0.454).otherwise(
            df["fuel_burn_landing3"]
        ),
    )
    return df


def calculate_fuel_burn_landing4_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_landing4_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_landing4"] * 0.454).otherwise(
            df["fuel_burn_landing4"]
        ),
    )
    return df


def calculate_fuel_burn_taxi_out1_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_taxi_out1_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_taxi_out1"] * 0.454).otherwise(
            df["fuel_burn_taxi_out1"]
        ),
    )
    return df


def calculate_fuel_burn_taxi_out2_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_taxi_out2_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_taxi_out2"] * 0.454).otherwise(
            df["fuel_burn_taxi_out2"]
        ),
    )
    return df


def calculate_fuel_burn_taxi_out3_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_taxi_out3_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_taxi_out3"] * 0.454).otherwise(
            df["fuel_burn_taxi_out3"]
        ),
    )
    return df


def calculate_fuel_burn_taxi_out4_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_taxi_out4_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_taxi_out4"] * 0.454).otherwise(
            df["fuel_burn_taxi_out4"]
        ),
    )
    return df


def calculate_fuel_burn_take_off1_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_take_off1_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_take_off1"] * 0.454).otherwise(
            df["fuel_burn_take_off1"]
        ),
    )
    return df


def calculate_fuel_burn_take_off2_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_take_off2_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_take_off2"] * 0.454).otherwise(
            df["fuel_burn_take_off2"]
        ),
    )
    return df


def calculate_fuel_burn_take_off3_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_take_off3_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_take_off3"] * 0.454).otherwise(
            df["fuel_burn_take_off3"]
        ),
    )
    return df


def calculate_fuel_burn_take_off4_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_take_off4_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_take_off4"] * 0.454).otherwise(
            df["fuel_burn_take_off4"]
        ),
    )
    return df


def calculate_fuel_burn_taxi_in1_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_taxi_in1_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_taxi_in1"] * 0.454).otherwise(
            df["fuel_burn_taxi_in1"]
        ),
    )
    return df


def calculate_fuel_burn_taxi_in2_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_taxi_in2_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_taxi_in2"] * 0.454).otherwise(
            df["fuel_burn_taxi_in2"]
        ),
    )
    return df


def calculate_fuel_burn_taxi_in3_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_taxi_in3_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_taxi_in3"] * 0.454).otherwise(
            df["fuel_burn_taxi_in3"]
        ),
    )
    return df


def calculate_fuel_burn_taxi_in4_kg(df, state, field_type):
    df = df.withColumn(
        "fuel_burn_taxi_in4_kg",
        F.when(df["vehicle"] == "B787", df["fuel_burn_taxi_in4"] * 0.454).otherwise(
            df["fuel_burn_taxi_in4"]
        ),
    )
    return df

def calculate_planned_taxi_time(df, state, field_type):
    df = df.withColumn(
        'planned_taxi_time',
        F.when(F.col('planned_taxi_time').isNotNull(), F.col('planned_taxi_time'))
         .otherwise(F.when((F.col('taxi_in_time_sched') + F.col('taxi_out_time_sched') > 0), 
                           F.col('taxi_in_time_sched') + F.col('taxi_out_time_sched')).otherwise(0)).cast(field_type)
    )
    
    if 'taxi_in_time_sched' not in df.columns:
        df = df.withColumn('planned_taxi_time', F.col('planned_taxi_time').fillna(0).cast('float'))
        
    return df

def calculate_taxi_in_minutes(df, state, field_type):
    df = df.withColumn(
        'taxi_in_minutes',
        F.when(
            (F.col('act_blon_time').isNotNull()) & (F.col('act_landing_time').isNotNull()),
            (F.unix_timestamp('act_blon_time') - F.unix_timestamp('act_landing_time')) / 60
        ).otherwise(F.col('taxi_in_minutes_measurement'))
    )
    
    return df    

def calculate_taxi_out_minutes(df, state, field_type):
    df = df.withColumn('act_takeoff_time', F.to_timestamp('act_takeoff_time'))
    df = df.withColumn('act_bloff_time', F.to_timestamp('act_bloff_time'))

    df = df.withColumn(
        'taxi_out_minutes',
        F.when(
            (F.col('act_takeoff_time').isNotNull()) &
            (F.col('act_bloff_time').isNotNull()) &
            ((F.col('act_takeoff_time').cast('long') - F.col('act_bloff_time').cast('long')) > 0),
            (F.col('act_takeoff_time').cast('long') - F.col('act_bloff_time').cast('long')) / 60
        ).otherwise(F.col('taxi_out_minutes_measurement'))
    )
    return df    

def calculate_act_taxi_in_fuel_engines(df, state, field_type):
    df = df.withColumn(
        'act_taxi_in_fuel_engines',
        F.col('fuel_burn_taxi_in1_kg') +
        F.col('fuel_burn_taxi_in2_kg') +
        F.col('fuel_burn_taxi_in3_kg') +
        F.col('fuel_burn_taxi_in4_kg')
    )
    return df    

def calculate_act_taxi_out_fuel_engines(df, state, field_type):
    df = df.withColumn(
        'act_taxi_out_fuel_engines',
        F.col('fuel_burn_taxi_out1_kg') +
        F.col('fuel_burn_taxi_out2_kg') +
        F.col('fuel_burn_taxi_out3_kg') +
        F.col('fuel_burn_taxi_out4_kg')
    )
    return df

def calculate_act_taxi_in_fuel(df, state, field_type):
    df = df.withColumn(
        'act_taxi_in_fuel',
        F.when((F.col('act_taxi_in_fuel_engines').isNotNull()) & (F.col('act_taxi_in_fuel_engines') > 0),
               F.col('act_taxi_in_fuel_engines')).otherwise(F.col('act_taxi_in_fuel_burn'))
    )
    return df

def calculate_act_taxi_out_fuel(df, state, field_type):
    df = df.withColumn(
        'act_taxi_out_fuel',
        F.when((F.col('act_taxi_out_fuel_engines').isNotNull()) & (F.col('act_taxi_out_fuel_engines') > 0),
               F.col('act_taxi_out_fuel_engines')).otherwise(F.col('act_taxi_out_fuel_burn'))
    )
    return df

def calculate_act_climb_fuel(df, state, field_type):
    df = df.withColumn(
        'act_climb_fuel',
        F.col('fuel_burn_climb1_kg') + F.col('fuel_burn_climb2_kg') + F.col('fuel_burn_climb3_kg') + F.col('fuel_burn_climb4_kg')
    )
    return df

def calculate_act_cruise_fuel(df, state, field_type):
    df = df.withColumn(
        'act_cruise_fuel',
        F.col('fuel_burn_cruise1_kg') + F.col('fuel_burn_cruise2_kg') + F.col('fuel_burn_cruise3_kg') + F.col('fuel_burn_cruise4_kg')
    )
    return df

def calculate_act_descent_fuel(df, state, field_type):
    df = df.withColumn(
        'act_descent_fuel',
        F.col('fuel_burn_descent1_kg') + F.col('fuel_burn_descent2_kg') + F.col('fuel_burn_descent3_kg') + F.col('fuel_burn_descent4_kg')
    )
    return df

def calculate_act_approach_fuel(df, state, field_type):
    df = df.withColumn(
        'act_approach_fuel',
        F.col('fuel_burn_approach1_kg') + F.col('fuel_burn_approach2_kg') + F.col('fuel_burn_approach3_kg') + F.col('fuel_burn_approach4_kg')
    )
    return df