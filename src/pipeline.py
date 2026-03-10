from db.run_migrations import run_all_migrations
import psycopg2

def main():

    conn = psycopg2.connect(
        host="localhost",
        dbname="olist_db",
        user="rijwan",
        password="rijwan123"
    )

    run_all_migrations(conn)

    print("Tables ready. Starting pipeline...")

if __name__ == "__main__":
    main()
