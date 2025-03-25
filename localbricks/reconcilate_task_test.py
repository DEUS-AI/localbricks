import re
import datetime
import math
import numpy as np
from dateutil import parser
from pprint import pprint as pp
from localbricks_factory import data_loader_manager 
from pyspark.sql.functions import col, max as spark_max


#plan_df = data_loader_manager(action= 'load_directly', share = 'pablo_local_dev', schema_table_name= "client-tui.reconciliation_temp_flight_plan_data")
#crew_df = data_loader_manager(action= 'load_directly', share = 'pablo_local_dev', schema_table_name= "client-tui.reconciliation_temp_flight_crew_data")
iot_df = data_loader_manager(action= 'load_directly', share = 'pablo_local_dev', schema_table_name= "client-tui.reconciliation_temp_flight_iot_data", limit=100000)

def compress_df_for_reconciliation(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.upper())

    # Define the regex patterns
    #time_pattern = re.compile(r'^(UTC|GMT){1}\s(HR|HRS|HOUR|MINUTE|MIN|SECOND|SEC|){1}(S)*\s*(X)?[0-9]*[^\(\)\/]*|^(NAV)(UTC|DATE)(HOUR|MIN|SEC)(S)*\_.*$')
    time_pattern = re.compile(r'^(UTC|GMT)_(HRS|HR|HOUR|MIN|MINUTE|SECS|SEC)(_.*)?$')
    date_pattern = re.compile(r'^(CAP CLOCK|DATE)\s*\(?\s*(DAY|MONTH|YEAR)\s*\)?|^(DAY|MONTH|YEAR)|^(NAVDATEDAY_|NAVDATEMONTH_|NAVDATEYEAR_).*$')

    flight_datetime_cols = [col for col in df.columns if time_pattern.match(col)]
    flight_date_cols = [col for col in df.columns if date_pattern.match(col)]

    # Combine the columns lists
    columns_to_maximize = flight_datetime_cols + flight_date_cols

    # Create a list of column expressions for aggregation
    aggregations = [spark_max(col).alias(col) for col in columns_to_maximize]

    # Perform the group by and aggregation
    grouped_df = df.groupBy("SOURCE_FILE_NAME").agg(*aggregations)
    return grouped_df

def extract_date(df):    
    flight_date_cols = [col for col in df.columns if re.match(r'^(CAP CLOCK|DATE)\s*\(?\s*(DAY|MONTH|YEAR)\s*\)?|^(DAY|MONTH|YEAR)|^(NAVDATEDAY_|NAVDATEMONTH_|NAVDATEYEAR_).*$', col)]
    
    if len(flight_date_cols) > 0:
        day = next((col for col in flight_date_cols if 'DAY' in col), None)
        month = next((col for col in flight_date_cols if 'MONTH' in col), None)
        year = next((col for col in flight_date_cols if 'YEAR' in col), None)
        if day is not None and month is not None and year is not None:
            flight_date_cols = [day, month, year]
        else:
            flight_date_cols = flight_date_cols[:3]
        
        dd_mm_yy = df[flight_date_cols].apply(lambda x: next((v for i, v in enumerate(x) if v is not None and int(v) > 0), None), axis=0) # [DD, MM, YY]
        flight_date = '-'.join(str(int(v)) for v in dd_mm_yy if v is not None) # DD-MM-YY
        if len(flight_date) < 6:
            if len(flight_date.split('-')) == 2:
                # only day and month (missing year)
                current_year = int(str(datetime.datetime.now().year)[-2:])
                flight_date = f'{flight_date}-{current_year}'
            else:
                raise Exception(f'Invalid flight date: {flight_date}')
            
        date = parser.parse(flight_date, dayfirst=True).date()
        return flight_date
    else:
        raise Exception("Unknown flight datetime fields")

def extract_time(data_frame, flight_date=None):
    time_pattern = re.compile(r'^(UTC|GMT)_(HRS|HR|HOUR|MIN|MINUTE|SECS|SEC)(_.*)?$')
    
    if flight_date is not None:
        # extract flight time from hh/mm/ss columns
        flight_start_time = None
        flight_datetime_cols = [col for col in df.columns if time_pattern.match(col)]
        
        if len(flight_datetime_cols) > 0:
            hours = next((col for col in flight_datetime_cols if ('HR' in col or 'HOUR' in col)), None)
            minutes = next((col for col in flight_datetime_cols if 'MIN' in col), None)
            seconds = next((col for col in flight_datetime_cols if 'SEC' in col), None)
            if hours is not None and minutes is not None:
                if seconds is not None:
                    flight_datetime_cols = [hours, minutes, seconds]
                else:
                    # hours
                    hour_cols = [col for col in flight_datetime_cols if ('HR' in col or 'HOUR' in col) and (' 1' in col or ' X1' in col)][:2] # hours (units + tens)
                    hour_tenx10 = [col for col in hour_cols if 'X10' in col]
                    if len(hour_tenx10) > 0:
                        data_frame[hour_tenx10] = data_frame[hour_tenx10].apply(lambda x: x * 10 if x < 10 else x * 1)
                    
                    # minutes
                    min_cols = [col for col in flight_datetime_cols if ('MIN' in col) and (' 1' in col or ' X1' in col)][:2]  # minutes (units + tens)
                    min_tenx10 = [col for col in min_cols if 'X10' in col]
                    
                    if len(min_tenx10) > 0:
                        data_frame[min_tenx10] = data_frame[min_tenx10].apply(lambda x: x * 10 if x < 10 else x * 1)
                    hours = data_frame[hour_cols].apply(lambda x: next((v for i, v in enumerate(x) if not np.isnan(v) and int(v) < 23), None), axis=0).sum()
                    minutes = data_frame[min_cols].apply(lambda x: next((v for i, v in enumerate(x) if not np.isnan(v) and int(v) < 59), None), axis=0).sum()
                    
                    if hours > 23 and minutes > 59:
                        raise Exception(f'Invalid flight time extracted {hours}:{minutes} from columns {flight_datetime_cols}')
                    flight_start_time = f'{int(hours)}:{int(minutes)}'
            
            else:
                flight_datetime_cols = flight_datetime_cols[:3]
            
            if flight_start_time is None:
                hh_mm_ss = data_frame[flight_datetime_cols].apply(lambda x: next((int(v) if v is not None else 0 for i, v in enumerate(x)), None), axis=0) # [HH, MM, SS]
                flight_start_time = ':'.join(str(int(v)) for v in hh_mm_ss if v is not None) # HH:MM:SS
            
            flight_start_time = flight_start_time.replace('.0', '')
            flight_datetime = f'{flight_date} {flight_start_time}'
            flight_datetime = parser.parse(flight_datetime, dayfirst=True)

            round_minutes = math.floor(flight_datetime.minute / 10) * 10
            plan_departure_datetime = flight_datetime.replace(minute=round_minutes, second=0)
            actual_departure_datetime = flight_datetime
            actual_arrival_datetime = flight_datetime + datetime.timedelta(seconds=30)
            
            print(f"Flight plan with {flight_date} plan_departure at: {plan_departure_datetime} actual_departure at: {actual_departure_datetime} arrival at: {actual_arrival_datetime}")
        else:
            raise Exception("Unknown flight datetime fields")
    else:
        raise Exception("Unknown flight date")

if __name__ == '__main__':
    df = compress_df_for_reconciliation(iot_df).toPandas()
    for index, row in df.iterrows():
        flight_date = extract_date(row.to_frame().T)
        extract_time(row.to_frame().T, flight_date)