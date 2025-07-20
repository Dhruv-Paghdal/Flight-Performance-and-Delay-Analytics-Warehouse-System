import pandas as pd
import random
import string
from datetime import time

input_csv = r"flights_2023.csv"
output_csv = r"flights_cleaned_2023.csv"

columns_to_read = [
    "FlightDate","Airline", "Tail_Number", "Dep_Airport", "Arr_Airport", "Distance_type", "Arr_Delay", "Dep_Delay",  "DepTime_label",   
]

airline_iata_mapping = {
    'Endeavor Air': '9E',
    'American Airlines Inc.': 'AA',
    'Alaska Airlines Inc.': 'AS',
    'JetBlue Airways': 'B6',
    'Delta Air Lines Inc': 'DL',
    'Frontier Airlines Inc.': 'F9',
    'Allegiant Air': 'G4',
    'Hawaiian Airlines Inc.': 'HA',
    'American Eagle Airlines Inc.': 'MQ',
    'Spirit Air Lines': 'NK',
    'Southwest Airlines Co.': 'WN',
    'Republic Airways': 'YX',
    'PSA Airlines': 'OH',
    'Skywest Airlines Inc.': 'OO',
    'United Air Lines Inc.': 'UA'
}

df = pd.read_csv(input_csv, usecols=columns_to_read, low_memory=False)

df["ORIGIN_AIRPORT"] =  df["Dep_Airport"]
df ["DESTINATION_AIRPORT"] = df["Arr_Airport"]

# Formate date
df['FlightDate'] = pd.to_datetime(df['FlightDate'], errors='coerce', format='%Y-%m-%d')
df['YEAR'] = df['FlightDate'].dt.year
df['MONTH'] = df['FlightDate'].dt.month
df['DAY'] = df['FlightDate'].dt.day

# Formate Airline
df['AIRLINE'] = df['Airline'].map(airline_iata_mapping)

# Formate tail number
def generate_tail_number(airline):
    if pd.isna(airline) or len(airline) < 2:
        suffix = 'XX'
    else:
        suffix = airline[-2:]

    prefix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
    return f'N{prefix}{suffix}'
df['TAIL_NUMBER'] = df.apply(
    lambda row: generate_tail_number(row['AIRLINE'])
    if pd.isna(row['Tail_Number']) or row['Tail_Number'] in [0, '0', '', 'NaN']
    else row['Tail_Number'],
    axis=1
)

# Formate Distance_type
def map_random_distance(dist_type):
    if dist_type == 'Short Haul >1500Mi':
        return random.randint(100, 150)
    elif dist_type == 'Medium Haul <3000Mi':
        return random.randint(100, 250)
    elif dist_type == 'Long Haul <6000Mi':
        return random.randint(200, 450)
    else:
        return None
df['DISTANCE'] = df['Distance_type'].apply(map_random_distance)

# Format Departure time
def generate_random_time(period):
    if period == 'Morning':
        hour = random.randint(5, 11)
    elif period == 'Afternoon':
        hour = random.randint(12, 16)
    elif period == 'Evening':
        hour = random.randint(17, 20)
    elif period == 'Night':
        hour = random.randint(21, 23) if random.random() < 0.5 else random.randint(0, 4)
    else:
        return None
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return time(hour, minute, second).strftime('%H:%M:%S')
df['DEPARTURE_TIME'] = df['DepTime_label'].apply(generate_random_time)

# Assign CANCELLED
df['CANCELLED'] = [random.choices([0, 1], weights=[0.9, 0.1])[0] for _ in range(len(df))]

# Possible reasons
reasons = ["Carrier", "Weather", "National Air System", "Security"]

# Assign CANCELLATION_REASON based on CANCELLED flag
df['CANCELLATION_REASON'] = df['CANCELLED'].apply(
    lambda x: random.choice(reasons) if x == 1 else "No Cancellation"
)

# Save output to final csv
final_df = df[["YEAR", "MONTH", "DAY", "AIRLINE", "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE", "ARRIVAL_DELAY", "DEPARTURE_DELAY",  "DEPARTURE_TIME", "CANCELLED", "CANCELLATION_REASON"]]
final_df.to_csv(output_csv, index=False)