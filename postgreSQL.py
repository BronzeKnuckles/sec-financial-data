# Written by CHATGPT, Improved my me
# Thanks ChatGPT !!!

import os
import psycopg2
import pandas as pd


# Function to create a table based on DataFrame dtypes
def create_table_from_df(df, table_name, cur):
    type_mapping = {
        "int64": "INTEGER",
        "float64": "REAL",
        "datetime64[ns]": "TIMESTAMP",
        "object": "TEXT",
    }

    columns = [f"{col} {type_mapping[str(df[col].dtype)]}" for col in df.columns]

    # Drop the table if it already exists (Tables were created previously, threw error, now fresh start)
    # cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
    cur.execute(create_table_sql)


def insert_into_postgresql(conn, cur, BASE_DIR):
    # Loop through each subdirectory and then each txt file
    for root, dirs, files in os.walk(BASE_DIR):
        for file in files:
            if file.endswith(".txt"):
                txt_file_path = os.path.join(root, file)

                # Read txt file into a DataFrame
                df = pd.read_csv(txt_file_path, sep="\t")
                # Extract table name from txt file name (customize this if needed)
                table_name = f"{os.path.splitext(file)[0]}_{root[7:]}"

                # Create table
                create_table_from_df(df, table_name, cur)

                # Use the COPY command
                with open(txt_file_path, "r") as f:
                    next(f)  # Skip the header row
                    cur.copy_from(f, table_name, null="")
                conn.commit()


def main():
    # PostgreSQL connection parameters
    DB_NAME = "sec-financial-data"
    DB_USER = "postgres"  # your user name
    DB_PASS = "root"  # your password
    DB_HOST = "localhost"
    DB_PORT = "5432"

    # Base directory containing subdirectories with txt files
    BASE_DIR = "./data/"

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()

    insert_into_postgresql(conn, cur, BASE_DIR)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
