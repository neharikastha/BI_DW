from library.Database import Database
from library.Variables import Variables
import os

file_name = os.path.basename(__file__).split('.')[0]

def load_stg_to_tmp():
    try:
        db = Database(file_name)
        print("Connected to the database")

        stg_table = f"{Variables.get_variable('STG_DB')}.stg_store"
        tmp_table = f"{Variables.get_variable('TMP_DB')}.tmp_store"

        db.execute_query("SET FOREIGN_KEY_CHECKS = 0;")

        # Truncate the tmp_table before inserting new data
        truncate_query = f"TRUNCATE TABLE {tmp_table}"
        db.execute_query(truncate_query)
        db.commit()
        print(f"Truncated table {tmp_table}")

        # Insert data from staging table to temp table
        insert_query = f"""
        INSERT INTO {tmp_table} (ID, REGION_ID, STORE_DESC)
        SELECT *
        FROM {stg_table} 

        db.execute_query(insert_query)
        db.commit()
        print(f"Data loaded from {stg_table} to {tmp_table}")
"""
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        try:
            db.execute_query("SET FOREIGN_KEY_CHECKS = 1;")
            db.disconnect()
        except Exception as e:
            print(f"Failed to disconnect: {e}")

if __name__ == "__main__":
    load_stg_to_tmp()
