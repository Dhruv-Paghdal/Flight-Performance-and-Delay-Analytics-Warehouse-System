import pandas as pd
import random
import string

input_csv = r"flights_usdot.csv"
output_csv = r"final_flights_usdot.csv"

columns_to_read = [
    "YEAR", "MONTH", "DAY", "AIRLINE", "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE", "ARRIVAL_DELAY", "DEPARTURE_DELAY",  "DEPARTURE_TIME", "CANCELLED", "CANCELLATION_REASON"   
]

df = pd.read_csv(input_csv, usecols=columns_to_read, low_memory=False)

# Funtion to replace Null values in tail_number
def generate_tail_number(airline):
    if pd.isna(airline) or len(airline) < 2:
        suffix = 'XX'  
    else:
        suffix = airline[-2:]  
    
    prefix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
    return f'N{prefix}{suffix}'
df['TAIL_NUMBER'] = df.apply(
    lambda row: generate_tail_number(row['AIRLINE']) if pd.isna(row['TAIL_NUMBER']) else row['TAIL_NUMBER'],
    axis=1
)

# Function to convert time to HH:MM:SS
def convert_to_hhmmss(time_val):
    if pd.isna(time_val):
        return "00:00:00"
    
    time_str = f"{int(time_val):04d}"
    
    hours = int(time_str[:-2])
    minutes = int(time_str[-2:])
    
    minutes = minutes % 60
    hours = (hours + minutes // 60) % 24
    
    return f"{hours:02d}:{minutes:02d}:00"
df['DEPARTURE_TIME'] = df['DEPARTURE_TIME'].apply(convert_to_hhmmss)

# Funtion to replace Null values in Arrival and Departure
df['DEPARTURE_DELAY'].fillna(0, inplace=True)
df['ARRIVAL_DELAY'].fillna(0, inplace=True)
def format_delay(val):
    if val < 0:
        return f"{int(abs(val))} minutes early"
    elif val > 0:
        return f"{int(val)} minutes late"
    else:
        return "On time"
df['DEPARTURE_DELAY'] = df['DEPARTURE_DELAY'].apply(format_delay)
df['ARRIVAL_DELAY'] = df['ARRIVAL_DELAY'].apply(format_delay)

# Funtion to replace Null value in CANCELLATION_REASON 
cancellation_reason_map = {
    "A": "Carrier", "B": "Weather", "C": "National Air System", "D": "Security"
}
def replace_cancellation_reason(row):
    if row['CANCELLED'] == 0:
        return "No Cancellation"
    else:
        return cancellation_reason_map.get(row['CANCELLATION_REASON'], "Unknown Reason")
df['CANCELLATION_REASON'] = df.apply(replace_cancellation_reason, axis=1)

# Save output to final csv
final_df = df[["YEAR", "MONTH", "DAY", "AIRLINE", "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE", "ARRIVAL_DELAY", "DEPARTURE_DELAY",  "DEPARTURE_TIME", "CANCELLED", "CANCELLATION_REASON"]]
final_df.to_csv(output_csv, index=False)