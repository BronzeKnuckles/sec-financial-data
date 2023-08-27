# SOURCE : CGATGPT
# Work in progress

import os
import psycopg2

# PostgreSQL connection parameters
DB_NAME = "sec-financial-data"
DB_USER = "postgres" # your user name 
DB_PASS = "root" # your password
DB_HOST = "localhost"
DB_PORT = "5432"

# Base directory containing subdirectories with CSV files
BASE_DIR = "./data/"

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# Loop through each subdirectory and then each CSV file
for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".csv"):
            csv_file_path = os.path.join(root, file)

            # Extract table name from CSV file name (customize this if needed)
            table_name = f"{root[7:]}_{os.path.splitext(file)[0]}"

            # Use the COPY command
            with open(csv_file_path, "r") as f:
                next(f)  # Skip the header row
                cur.copy_expert(f, table_name)
                cur.copy_expert(
                    file=f,
                    table_name=table_name,
                )
            conn.commit()

cur.close()
conn.close()
