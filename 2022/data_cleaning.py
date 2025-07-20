import pandas as pd

# Load the original flight.csv
df = pd.read_csv("flight.csv")

# Clean column names just in case
df.columns = df.columns.str.strip()

# Delay formatting function
def format_delay(value):
    try:
        val = float(value)
        if val < 0:
            return f"{abs(int(val))} minutes early"
        elif val > 0:
            return f"{int(val)} minutes late"
        else:
            return "On time"
    except:
        return "Unknown"

# Replace Arrival_Delay and Departure_Delay with formatted values
if "Arrival_Delay" in df.columns:
    df["Arrival_Delay"] = df["Arrival_Delay"].apply(format_delay)
else:
    print("⚠'Arrival_Delay' column not found.")

if "Departure_Delay" in df.columns:
    df["Departure_Delay"] = df["Departure_Delay"].apply(format_delay)
else:
    print("⚠'Departure_Delay' column not found.")

# Save to a new file
df.to_csv("flight_formatted.csv", index=False)
print(" Replaced delay values saved to 'flight_formatted.csv'.")
