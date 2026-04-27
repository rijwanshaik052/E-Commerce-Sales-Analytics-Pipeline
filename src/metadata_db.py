# # import psycopg2
# # import logging

# # logger = logging.getLogger(__name__)

# # def get_connection():
# #     try:
# #         conn = psycopg2.connect(
# #             dbname='olist_db',
# #             user='postgres',
# #             password='postgres_password',
# #             host='localhost',
# #             port='5432'
# #         )
# #         return conn
# #     except Exception as e:
# #         logger.error(f"Error connecting to the database: {e}")
# #         raise

# # def create_table():
# #     conn = get_connection()
# #     try:
# #         with conn.cursor() as cur:
# #             cur.execute("""
# #                 CREATE TABLE IF NOT EXISTS file_metadata (
# #                     id SERIAL PRIMARY KEY,
# #                     file_name VARCHAR(255) NOT NULL,
# #                     file_size BIGINT NOT NULL,
# #                     upload_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
# #                     file_type VARCHAR(50) NOT NULL,
# #                     file_path VARCHAR(255) NOT NULL,
# #                     S3_KEY VARCHAR(255),
# #                     row_count BIGINT,
# #                     error_message TEXT                  
                    
# #                 );
# #             """)
# #             conn.commit()
# #             logger.info("Metadata table created successfully.")
# #     except Exception as e:
# #         logger.error(f"Error creating table: {e}")
# #         raise
# #     finally:
# #         conn.close()

# # def insert_metadata(key, value):
# #     conn = get_connection()
# #     try:
# #         with conn.cursor() as cur:
# #             cur.execute("""
# #                 INSERT INTO file_metadata (key, value) VALUES (%s, %s);
# #             """, (key, value))
# #             conn.commit()
# #             logger.info(f"Metadata inserted: {key} = {value}")
# #     except Exception as e:
# #         logger.error(f"Error inserting metadata: {e}")
# #         raise
# #     finally:
# #         conn.close()

# # def get_metadata(key):
# #     conn = get_connection()
# #     try:
# #         with conn.cursor() as cur:
# #             cur.execute("""
# #                 SELECT value FROM metadata WHERE key = %s;
# #             """, (key,))
# #             result = cur.fetchone()
# #             if result:
# #                 logger.info(f"Metadata retrieved: {key} = {result[0]}")
# #                 return result[0]
# #             else:
# #                 logger.warning(f"No metadata found for key: {key}")
# #                 return None
# #     except Exception as e:
# #         logger.error(f"Error retrieving metadata: {e}")
# #         raise
# #     finally:
# #         conn.close() 

   
# # if __name__ == "__main__":
# #     create_table()
# #     insert_metadata('example_key', 'example_value')
# #     value = get_metadata('example_key')
# #     print(f"Retrieved metadata: {value}")   


# import psycopg2
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# def get_connection():
#     return psycopg2.connect(
#         dbname="olist_db",
#         user="postgres",
#         password="postgres_password",
#         host="localhost",
#         port="5432"
#     )


# def create_table():
#     conn = get_connection()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("""
#                 CREATE TABLE IF NOT EXISTS file_metadata (
#                     id SERIAL PRIMARY KEY,
#                     file_name VARCHAR(255) NOT NULL,
#                     file_size BIGINT NOT NULL,
#                     upload_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
#                     file_type VARCHAR(50) NOT NULL,
#                     file_path VARCHAR(255) NOT NULL,
#                     s3_key VARCHAR(255),
#                     row_count BIGINT,
#                     status VARCHAR(50) NOT NULL,
#                     error_message TEXT
#                 );
#             """)
#             conn.commit()
#             logger.info("file_metadata table created successfully.")
#     except Exception as e:
#         conn.rollback()
#         logger.error(f"Error creating table: {e}")
#         raise
#     finally:
#         conn.close()


# def insert_file_metadata(
#     file_name,
#     file_size,
#     file_type,
#     file_path,
#     s3_key,
#     row_count,
#     status,
#     error_message=None
# ):
#     conn = get_connection()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("""
#                 INSERT INTO file_metadata (
#                     file_name,
#                     file_size,
#                     file_type,
#                     file_path,
#                     s3_key,
#                     row_count,
#                     status,
#                     error_message
#                 )
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
#             """, (
#                 file_name,
#                 file_size,
#                 file_type,
#                 file_path,
#                 s3_key,
#                 row_count,
#                 status,
#                 error_message
#             ))
#             conn.commit()
#             logger.info("Metadata inserted for file: %s", file_name)
#     except Exception as e:
#         conn.rollback()
#         logger.error(f"Error inserting metadata: {e}")
#         raise
#     finally:
#         conn.close()


# if __name__ == "__main__":
#     create_table()

#     for file in files:
#         row_count = 0  # Replace with actual row count logic if available
#         insert_file_metadata(
#             file_name=file.name,
#             file_size=file.stat().st_size,
#             file_type=file.suffix,
#             file_path=str(file),
#         s3_key=f"bronze/{file.name}",
#         row_count=row_count,
#         status="SUCCESS"
#     )




import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(
        dbname="olist_db",
        user="postgres",
        password="postgres_password",
        host="localhost",
        port="5433"
    )


def create_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS file_metadata (
                    id SERIAL PRIMARY KEY,
                    file_name VARCHAR(255) NOT NULL,
                    file_size BIGINT NOT NULL,
                    upload_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    file_type VARCHAR(50) NOT NULL,
                    file_path VARCHAR(255) NOT NULL,
                    s3_key VARCHAR(255),
                    row_count BIGINT,
                    status VARCHAR(50) NOT NULL,
                    error_message TEXT
                );
            """)
            conn.commit()
            logger.info("file_metadata table created successfully.")

    except Exception as e:
        conn.rollback()
        logger.error("Error creating table: %s", e)
        raise

    finally:
        conn.close()


def insert_file_metadata(
    file_name,
    file_size,
    file_type,
    file_path,
    s3_key,
    row_count,
    status,
    error_message=None
):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO file_metadata (
                    file_name,
                    file_size,
                    file_type,
                    file_path,
                    s3_key,
                    row_count,
                    status,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                file_name,
                file_size,
                file_type,
                file_path,
                s3_key,
                row_count,
                status,
                error_message
            ))

            conn.commit()
            logger.info("Metadata inserted for file: %s", file_name)

    except Exception as e:
        conn.rollback()
        logger.error("Error inserting metadata: %s", e)
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    create_table()