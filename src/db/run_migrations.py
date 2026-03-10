import psycopg2
from pathlib import Path

def run_sql_file(conn, path):
    with open(path, "r") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def run_all_migrations(conn):

    migration_files = [
        "migrations/1_create_schema.sql",
        "migrations/2_create_metadata_table.sql",
        "migrations/3_create_raw_tables.sql",
        "migrations/4_create_staging_tables.sql",
        "migrations/5_create_clean_tables.sql"
    ]

    for file in migration_files:
        print(f"Running {file}")
        run_sql_file(conn, file)
