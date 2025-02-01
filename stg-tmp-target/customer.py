from library.Database import Database
import os
from library.Variables import Variables

file_name = os.path.basename(__file__).split('.')[0]

def load_stg_to_tmp():
    try:
        db = Database(file_name)
        print("Connected to the database")

        stg_table = f"{Variables.get_variable('STG_DB')}.stg_{file_name}"
        tmp_table = f"{Variables.get_variable('TMP_DB')}.tmp_{file_name}"
        tgt_table = f"{Variables.get_variable('TGT_DB')}.d_retail_customer_t"

        db.execute_query("SET FOREIGN_KEY_CHECKS = 0;")

        # Truncate the tmp_table before inserting new data
        truncate_query = f"TRUNCATE TABLE {tmp_table}"
        db.execute_query(truncate_query)
        db.commit()
        print(f"Truncated table {tmp_table}")

        # Insert data from staging table to temp table
        insert_query = f"""
        INSERT INTO {tmp_table} 
        SELECT * FROM {stg_table}
        """
        db.execute_query(insert_query)
        db.commit()
        print(f"Data loaded from {stg_table} to {tmp_table}")


        truncate_query = f"TRUNCATE TABLE {tgt_table}"
        db.execute_query(truncate_query)
        db.commit()
        print(f"Truncated table {tgt_table}")

        insert_query = f"""
        INSERT INTO {tgt_table} (CUSTOMER_ID,CUSTOMER_FST_NM, CUSTOMER_MID_NM, CUSTOMER_LST_NM, CUSTOMER_ADDR, ROW_INSRT_TMS, ROW_UPDT_TMS)
        SELECT 
            ID,
          CUSTOMER_FIRST_NAME, 
          CUSTOMER_MIDDLE_NAME, 
          CUSTOMER_LAST_NAME, 
          CUSTOMER_ADDRESS, 
          CURRENT_TIMESTAMP, 
          CURRENT_TIMESTAMP
        FROM {tmp_table};
        """

        db.execute_query(insert_query)
        db.commit()
        print(f"Data loaded from {tmp_table} to {tgt_table}")

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
