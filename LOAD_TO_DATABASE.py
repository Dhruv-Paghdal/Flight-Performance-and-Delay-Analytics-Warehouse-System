import pandas as pd
import pyodbc

# Change parameter for each year data
AIRLINE_CSV = r"final_airlines_2015.csv"
AIRPORT_CSV = r"final_airports_2015.csv"
FLIGHT_CSV = r"flights_cleaned_2015.csv"
TARGET_DATABASE = "flight_data_2015"
FLIGHT_ROW_LIMIT = 1450000
BATCH_SIZE = 10000

conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=localhost\SQLEXPRESS;"
    f"Database={TARGET_DATABASE};"
    r"Trusted_Connection=yes;"
)
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # Optimize executemany
    cursor.fast_executemany = True
except pyodbc.Error as e:
    exit(1)

try:
    airline_df = pd.read_csv(AIRLINE_CSV)
    airport_df = pd.read_csv(AIRPORT_CSV)
    flight_df = pd.read_csv(FLIGHT_CSV, nrows=FLIGHT_ROW_LIMIT)
except Exception as e:
    cursor.close()
    conn.close()
    exit(1)

try:
    data = [tuple(row) for row in airline_df[['IATA_CODE', 'AIRLINE']].values]
    cursor.executemany(
        "INSERT INTO AIRLINE (IATA_CODE, AIRLINE) VALUES (?, ?)",
        data
    )
    conn.commit()
except pyodbc.Error as e:
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

try:
    data = [tuple(row) for row in airport_df[['IATA_CODE', 'AIRPORT', 'CITY', 'STATE']].values]
    cursor.executemany(
        "INSERT INTO AIRPORT (IATA_CODE, AIRPORT, CITY, STATE) VALUES (?, ?, ?, ?)",
        data
    )
    conn.commit()
except pyodbc.Error as e:
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

try:
    cursor.execute("SELECT IATA_CODE FROM AIRLINE")
    valid_airlines = set(row[0] for row in cursor.fetchall())
    cursor.execute("SELECT IATA_CODE FROM AIRPORT")
    valid_airports = set(row[0] for row in cursor.fetchall())

    flight_df = flight_df[
        (flight_df['AIRLINE'].isin(valid_airlines)) &
        (flight_df['ORIGIN_AIRPORT'].isin(valid_airports)) &
        (flight_df['DESTINATION_AIRPORT'].isin(valid_airports)) &
        (~flight_df.index.isin(range(218995, 219006)))
    ]
except pyodbc.Error as e:
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

try:
    expected_columns = [
        'YEAR', 'MONTH', 'DAY', 'AIRLINE', 'TAIL_NUMBER', 'ORIGIN_AIRPORT',
        'DESTINATION_AIRPORT', 'DISTANCE', 'ARRIVAL_DELAY', 'DEPARTURE_DELAY',
        'DEPARTURE_TIME', 'CANCELLED', 'CANCELLATION_REASON'
    ]
    if not all(col in flight_df.columns for col in expected_columns):
        missing = [col for col in expected_columns if col not in flight_df.columns]
        raise ValueError(f"Missing columns in flight_df: {missing}")

    flight_df = flight_df[expected_columns]

    invalid_times = flight_df[~flight_df['DEPARTURE_TIME'].str.match(r'^\d{2}:\d{2}:\d{2}$', na=False)]
    if not invalid_times.empty:
        flight_df.loc[~flight_df['DEPARTURE_TIME'].str.match(r'^\d{2}:\d{2}:\d{2}$', na=False), 'DEPARTURE_TIME'] = None

    data = [tuple(row) for row in flight_df.values]

    # Insert in batches
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        cursor.executemany(
            """
            INSERT INTO FLIGHT (
                YEAR, MONTH, DAY, AIRLINE, TAIL_NUMBER, ORIGIN_AIRPORT,
                DESTINATION_AIRPORT, DISTANCE, ARRIVAL_DELAY, DEPARTURE_DELAY,
                DEPARTURE_TIME, CANCELLED, CANCELLATION_REASON
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch
        )
        conn.commit()

except pyodbc.Error as e:
    print(f"Error inserting into FLIGHT: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)
except Exception as e:
    print(f"Error preparing data for FLIGHT insert: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

conn.commit()
cursor.close()
conn.close()
print("All data inserted and connection closed.")