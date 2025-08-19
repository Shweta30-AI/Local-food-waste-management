import pandas as pd
import sqlite3
import os

# Paths to CSV files
base_path = r"C:/Users/Shweta/OneDrive/Desktop/local-food-waste"
providers_csv = os.path.join(base_path, "providers_data.csv")
receivers_csv = os.path.join(base_path, "receivers_data.csv")
food_listings_csv = os.path.join(base_path, "food_listings_data.csv")
claims_csv = os.path.join(base_path, "claims_data.csv")

# Load CSV files into pandas DataFrames
providers_df = pd.read_csv(providers_csv)
receivers_df = pd.read_csv(receivers_csv)
food_listings_df = pd.read_csv(food_listings_csv)
claims_df = pd.read_csv(claims_csv)

# Clean data: strip spaces in column names
providers_df.columns = providers_df.columns.str.strip()
receivers_df.columns = receivers_df.columns.str.strip()
food_listings_df.columns = food_listings_df.columns.str.strip()
claims_df.columns = claims_df.columns.str.strip()

# Convert date columns to proper datetime
food_listings_df['Expiry_Date'] = pd.to_datetime(food_listings_df['Expiry_Date'], errors='coerce')
claims_df['Timestamp'] = pd.to_datetime(claims_df['Timestamp'], errors='coerce')

# Create SQLite database
db_path = os.path.join(base_path, "food_waste.db")
conn = sqlite3.connect(db_path)

# Save DataFrames to SQL tables
providers_df.to_sql("providers", conn, if_exists="replace", index=False)
receivers_df.to_sql("receivers", conn, if_exists="replace", index=False)
food_listings_df.to_sql("food_listings", conn, if_exists="replace", index=False)
claims_df.to_sql("claims", conn, if_exists="replace", index=False)

# Commit and close
conn.commit()
conn.close()

print("Database created successfully at:", db_path)
