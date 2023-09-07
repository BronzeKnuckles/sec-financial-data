import psycopg2
from multiprocessing import Pool, cpu_count


# Made Table with Query tool in pgadmin

"""CREATE TABLE main (
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


def insert_data_from_table(table_name):
    # Each process establishes its own connection
    conn = psycopg2.connect(
        dbname="sec-financial-data",
        user="postgres",
        password="root",
        host="localhost",
        port="5432",
    )
    cur = conn.cursor()

    # Get the column list from one of the tables
    cur.execute(
        """
        SELECT string_agg(column_name, ', ')
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '_cu_bancorp'
    """
    )
    column_list = cur.fetchone()[0]
    column_list = column_list.replace("fy", "fy::character varying(20)")

    # Insert data from the table into the main table
    insert_sql = f"INSERT INTO main SELECT {column_list} FROM {table_name}"
    cur.execute(insert_sql)
    conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="sec-financial-data",
        user="postgres",
        password="root",
        host="localhost",
        port="5432",
    )
    cur = conn.cursor()

    # Get the list of tables to insert from
    cur.execute(
        """
        SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT LIKE 'main'
    """
    )
    tables = [item[0] for item in cur.fetchall()]

    cur.close()
    conn.close()

    # Use multiprocessing to parallelize the insertion
    with Pool(processes=cpu_count()) as pool:
        pool.map(insert_data_from_table, tables)
