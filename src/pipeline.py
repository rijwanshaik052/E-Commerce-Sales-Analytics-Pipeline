# from db.run_migrations import run_all_migrations
# import psycopg2

# def main():

#     conn = psycopg2.connect(
#         host="localhost",
#         dbname="olist_db",
#         user="rijwan",
#         password="rijwan123"
#     )

#     run_all_migrations(conn)

#     print("Tables ready. Starting pipeline...")

# if __name__ == "__main__":
#     main()




# import time
# import psycopg2
# from db.run_migrations import run_all_migrations

# def connect_db():
#     while True:
#         try:
#             conn = psycopg2.connect(
#                 host="postgres",
#                 port=5432,
#                 dbname="olist_db",
#                 user="rijwan",
#                 password="rijwan123"
#             )
#             print("Connected to Postgres")
#             return conn
#         except psycopg2.OperationalError:
#             print("Postgres not ready... retrying")
#             time.sleep(5)

#             run_all_migrations(conn)

#             print("Tables ready. Starting pipeline...")



# def main():
#     conn = connect_db()
#     # continue pipeline

# if __name__ == "__main__":
#     main()



from src.ingest_scripts.scan_incoming_data import scan_incoming_files

files = scan_incoming_files("/home/rijwan/Desktop/E-Commerce-Sales-Analytics-Pipeline/data/incoming")

for f in files:
    print(f)












# import os
# import psycopg2

# def main():

#     conn = psycopg2.connect(
#         host=os.getenv("DB_HOST"),
#         port=os.getenv("DB_PORT"),
#         database=os.getenv("DB_NAME"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD")
#     )

#     print("Connected to Postgres")

#     conn.close()

# if __name__ == "__main__":
#     main()
