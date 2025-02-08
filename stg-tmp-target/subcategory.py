from library.Database import Database
from library.Variables import Variables
import os

file_name = os.path.basename(__file__).split('.')[0]

def load_stg_to_tmp():
    try:
        db = Database(file_name)
        print("Connected to the database")

        stg_table = f"{Variables.get_variable('STG_DB')}.stg_{file_name}"
        tmp_table = f"{Variables.get_variable('TMP_DB')}.tmp_{file_name}"
        tgt_table = f"{Variables.get_variable('TGT_DB')}"

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

        # Truncate the tmp_table before inserting new data
        truncate_query = f"TRUNCATE TABLE {tgt_table}.d_retail_sub_ctgry_t"
        db.execute_query(truncate_query)
        db.commit()
        print(f"Truncated table {tgt_table}.d_retail_sub_ctgry_t")

        insert_query = f"""
        INSERT INTO {tgt_table}.d_retail_sub_ctgry_t (SUB_CTGRY_ID, CTGRY_KY, SUB_CTGRY_DESC, Row_INSRT_TMS, Row_UPDT_TMS)
                SELECT 
                    S.ID,
                    COALESCE(C.CTGRY_KY, -1) AS CTGRY_KY,  -- Replace NULL with -1
                    S.SUBCATEGORY_DESC,
                    CURRENT_TIMESTAMP as row_insrt_tms,
                    CURRENT_TIMESTAMP as row_updt_tms
                FROM {tmp_table} S
                LEFT JOIN {tgt_table}.d_retail_ctgry_t C
                ON S.ID = C.CTGRY_ID
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
