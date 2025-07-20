import pyodbc
import re
from datetime import time

TARGET_DATABASE = "flight_dataWarehouse"
BATCH_SIZE = 50000 

def connect_to_db(database):
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost\SQLEXPRESS;"
        f"Database={database};"
        r"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        # Optimize execute many
        cursor.fast_executemany = True 
        print(f"Connected to database: {database}")
        return conn, cursor
    except pyodbc.Error as e:
        print(f"Connection failed for {database}: {e}")
        return None, None

def create_data_warehouse_tables(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimDate' AND xtype='U')
        CREATE TABLE DimDate (
            DateKey INT PRIMARY KEY,
            Year INT,
            Month INT,
            Day INT
        )
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimAirline' AND xtype='U')
        CREATE TABLE DimAirline (
            AirlineKey INT IDENTITY(1,1) PRIMARY KEY,
            IATA_CODE VARCHAR(10) UNIQUE,
            Airline VARCHAR(100)
        )
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimAirport' AND xtype='U')
        CREATE TABLE DimAirport (
            AirportKey INT IDENTITY(1,1) PRIMARY KEY,
            IATA_CODE VARCHAR(10) UNIQUE,
            Airport VARCHAR(100),
            City VARCHAR(50),
            State VARCHAR(50)
        )
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FactFlight' AND xtype='U')
        CREATE TABLE FactFlight (
            FlightID INT IDENTITY(1,1) PRIMARY KEY,
            DateKey INT,
            AirlineKey INT,
            OriginAirportKey INT,
            DestAirportKey INT,
            Distance INT,
            ArrivalDelay INT,
            DepartureDelay INT,
            CancelledFlag BIT,
            DepartureTime TIME,
            CancelReason VARCHAR(50),
            FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
            FOREIGN KEY (AirlineKey) REFERENCES DimAirline(AirlineKey),
            FOREIGN KEY (OriginAirportKey) REFERENCES DimAirport(AirportKey),
            FOREIGN KEY (DestAirportKey) REFERENCES DimAirport(AirportKey)
        )
    """)

def extract_data(cursor, table_name, limit=None):
    query = f"SELECT * FROM {table_name}"
    if limit is not None:
        query = f"SELECT TOP {limit} * FROM {table_name}"
    try:
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        if table_name == "FLIGHT" and rows:
            departure_time_index = columns.index('DEPARTURE_TIME')
            sample_types = [type(row[departure_time_index]) for row in rows[:10]]
        return rows, columns
    except pyodbc.Error as e:
        print(f"Error extracting data from {table_name}: {e}")
        return [], []

def transform_data(flight_data, airline_data, airport_data, columns_flight, columns_airline, columns_airport):
    # DimDate
    dim_date_data = {}
    for row in flight_data:
        year = row[columns_flight.index('YEAR')]
        month = row[columns_flight.index('MONTH')]
        day = row[columns_flight.index('DAY')]
        date_key = int(f"{year}{month:02d}{day:02d}")
        if date_key not in dim_date_data:
            dim_date_data[date_key] = (date_key, year, month, day)

    # DimAirline
    dim_airline_data = set()
    for row in airline_data:
        iata = row[columns_airline.index('IATA_CODE')]
        airline = row[columns_airline.index('AIRLINE')]
        dim_airline_data.add((iata, airline))

    # DimAirport
    dim_airport_data = set()
    for row in airport_data:
        iata = row[columns_airport.index('IATA_CODE')]
        airport = row[columns_airport.index('AIRPORT')]
        city = row[columns_airport.index('CITY')]
        state = row[columns_airport.index('STATE')]
        dim_airport_data.add((iata, airport, city, state))

    # FactFlight
    fact_flight_data = []
    invalid_times = []
    for i, row in enumerate(flight_data):
        year = row[columns_flight.index('YEAR')]
        month = row[columns_flight.index('MONTH')]
        day = row[columns_flight.index('DAY')]
        date_key = int(f"{year}{month:02d}{day:02d}")

        airline_iata = row[columns_flight.index('AIRLINE')]
        origin_iata = row[columns_flight.index('ORIGIN_AIRPORT')]
        dest_iata = row[columns_flight.index('DESTINATION_AIRPORT')]

        distance = row[columns_flight.index('DISTANCE')]
        arrival_delay = row[columns_flight.index('ARRIVAL_DELAY')]
        departure_delay = row[columns_flight.index('DEPARTURE_DELAY')]
        cancelled = 1 if row[columns_flight.index('CANCELLED')] else 0
        departure_time = row[columns_flight.index('DEPARTURE_TIME')]
        cancel_reason = row[columns_flight.index('CANCELLATION_REASON')]

        # Handle DEPARTURE_TIME
        if isinstance(departure_time, time):
            departure_time = departure_time.strftime('%H:%M:%S.%f')[:-3]
        elif isinstance(departure_time, str) and departure_time.strip() and departure_time.strip().lower() != 'null':
            if not bool(re.match(r'^\d{2}:\d{2}:\d{2}(\.\d{1,7})?$', departure_time)):
                invalid_times.append((i, departure_time))
                departure_time = None
        else:
            invalid_times.append((i, departure_time))
            departure_time = None

        fact_flight_data.append((
            date_key, airline_iata, origin_iata, dest_iata,
            distance, arrival_delay, departure_delay, cancelled, departure_time, cancel_reason
        ))

    return list(dim_date_data.values()), dim_airline_data, dim_airport_data, fact_flight_data

def load_data(cursor, dim_date_data, dim_airline_data, dim_airport_data, fact_flight_data):
    try:
        cursor.execute("SELECT DateKey FROM DimDate")
        existing_dates = {row[0] for row in cursor.fetchall()}
        dates_to_insert = [rec for rec in dim_date_data if rec[0] not in existing_dates]
        if dates_to_insert:
            cursor.executemany(
                "INSERT INTO DimDate (DateKey, Year, Month, Day) VALUES (?, ?, ?, ?)",
                dates_to_insert
            )

        cursor.execute("SELECT IATA_CODE FROM DimAirline")
        existing_airlines = {row[0] for row in cursor.fetchall()}
        airlines_to_insert = [rec for rec in dim_airline_data if rec[0] not in existing_airlines]
        if airlines_to_insert:
            cursor.executemany(
                "INSERT INTO DimAirline (IATA_CODE, Airline) VALUES (?, ?)",
                airlines_to_insert
            )
            
        cursor.execute("SELECT IATA_CODE FROM DimAirport")
        existing_airports = {row[0] for row in cursor.fetchall()}
        airports_to_insert = [rec for rec in dim_airport_data if rec[0] not in existing_airports]
        if airports_to_insert:
            cursor.executemany(
                "INSERT INTO DimAirport (IATA_CODE, Airport, City, State) VALUES (?, ?, ?, ?)",
                airports_to_insert
            )
            
        cursor.connection.commit()

        cursor.execute("SELECT AirlineKey, IATA_CODE FROM DimAirline")
        airline_map = {iata: key for key, iata in cursor.fetchall()}
        cursor.execute("SELECT AirportKey, IATA_CODE FROM DimAirport")
        airport_map = {iata: key for key, iata in cursor.fetchall()}
        cursor.execute("SELECT DateKey FROM DimDate")
        date_map = {key[0]: key[0] for key in cursor.fetchall()}

        total_inserted = 0
        batch_params = []
        skipped_rows = 0

        for i, row in enumerate(fact_flight_data):
            (date_key, airline_iata, origin_iata, dest_iata,
             distance, arrival_delay, departure_delay, cancelled, departure_time, cancel_reason) = row

            airline_key = airline_map.get(airline_iata)
            origin_key = airport_map.get(origin_iata)
            dest_key = airport_map.get(dest_iata)

            if airline_key and origin_key and dest_key and date_key in date_map:
                batch_params.append((
                    date_key, airline_key, origin_key, dest_key, distance,
                    arrival_delay, departure_delay, cancelled, departure_time, cancel_reason
                ))
            else:
                skipped_rows += 1

            if len(batch_params) >= BATCH_SIZE:
                cursor.executemany(
                    """
                    INSERT INTO FactFlight
                    (DateKey, AirlineKey, OriginAirportKey, DestAirportKey, Distance,
                     ArrivalDelay, DepartureDelay, CancelledFlag, DepartureTime, CancelReason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch_params
                )
                total_inserted += len(batch_params)
                batch_params.clear()

        if batch_params:
            cursor.executemany(
                """
                INSERT INTO FactFlight
                (DateKey, AirlineKey, OriginAirportKey, DestAirportKey, Distance,
                 ArrivalDelay, DepartureDelay, CancelledFlag, DepartureTime, CancelReason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch_params
            )
            total_inserted += len(batch_params)

        cursor.connection.commit()
        print(f"Inserted total {total_inserted} rows into FactFlight.")

    except pyodbc.Error as e:
        print(f"Error loading data: {e}")
        cursor.connection.rollback()
        raise

def etl_process(years, drop_target=False):
    # Connect to target data warehouse
    target_conn, target_cursor = connect_to_db(TARGET_DATABASE)
    if not target_conn:
        return

    try:
        # Create data warehouse tables
        create_data_warehouse_tables(target_cursor)
        target_conn.commit()

        for year in years:
            source_db = f"flight_data_{year}"
            source_conn, source_cursor = connect_to_db(source_db)
            if not source_conn:
                continue

            try:
                # Extract data
                flight_data, flight_cols = extract_data(source_cursor, "FLIGHT")
                airline_data, airline_cols = extract_data(source_cursor, "AIRLINE")
                airport_data, airport_cols = extract_data(source_cursor, "AIRPORT")

                # Transform data
                dim_date_data, dim_airline_data, dim_airport_data, fact_flight_data = transform_data(
                    flight_data, airline_data, airport_data,
                    flight_cols, airline_cols, airport_cols
                )

                # Load data
                load_data(target_cursor, dim_date_data, dim_airline_data, dim_airport_data, fact_flight_data)

                source_cursor.close()
                source_conn.close()
            except Exception as e:
                print(f"Error processing year {year}: {e}")
                source_cursor.close()
                source_conn.close()
                continue

        # Verify total flights loaded
        target_cursor.execute("SELECT COUNT(*) FROM FactFlight")
        total_flights = target_cursor.fetchone()[0]

    except Exception as e:
        print(f"ETL process failed: {e}")
        target_conn.rollback()
    finally:
        target_cursor.close()
        target_conn.close()

    print("ETL process completed.")

if __name__ == "__main__":
    # Load all data for these years
    years_to_test = [2015, 2022, 2023]  
    etl_process(years_to_test, drop_target=False)