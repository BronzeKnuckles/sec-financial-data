import os
import re
import psycopg2
import pandas as pd

# Database connection parameters
DB_PARAMS = {
    "dbname": "sec-financial-data",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": "5432",
}

DATA_DIR = "./data/companyfacts/"
csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

# Connect to the PostgreSQL database
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()


def check_not_char_underscore(string):
    if re.match("^[^A-Za-z_]", string):
        return True
    else:
        return False


def check_data_integrity(path):
    df = pd.read_csv(path)
    try:
        del df["end"]
        del df["filed"]
    except:
        pass
    df.to_csv(path, index=False)


for file in csv_files:
    check_data_integrity(os.path.join(DATA_DIR, file))

    with open(os.path.join(DATA_DIR, file), "r") as f:
        table_name = file
        if check_not_char_underscore:
            table_name = "_" + table_name
        table_name = table_name.replace(",", "")
        table_name = table_name.replace(".", "")
        table_name = table_name.replace(" ", "_")
        table_name = table_name.replace("-", "_")
        table_name = table_name.replace("&", "and")
        table_name = table_name.replace("'", "")
        table_name = table_name.replace("#", "")
        table_name = table_name.replace("csv", "")
        table_name = table_name.replace("(", "")
        table_name = table_name.replace(")", "")
        table_name = table_name.replace("!", "")

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                CompanyName VARCHAR(255),
                cik INT,
                fact VARCHAR(255),
                units VARCHAR(20),
                val FLOAT,
                accn VARCHAR(255),
                fy VARCHAR(20),
                fp VARCHAR(10),
                form VARCHAR(10)
            );
        """
        cur.execute(create_table_sql)
        copy_sql = f"""
           COPY {table_name} FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """
        cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()

cur.close()
conn.close()
