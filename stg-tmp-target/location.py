from library.Database import Database
import os
from library.Variables import Variables

file_name = os.path.basename(__file__).split('.')[0]

def load_stg_to_tmp():
    try:
        db = Database(file_name)
        print("Connected to the database")

        # stg_table = f"{Variables.get_variable('STG_DB')}.stg_{file_name}"
        tmp_table = f"{Variables.get_variable('TMP_DB')}.tmp_store"
        tgt_table = f"{Variables.get_variable('TGT_DB')}"

        db.execute_query("SET FOREIGN_KEY_CHECKS = 0;")

        # Truncate the tmp_table before inserting new data
        truncate_query = f"TRUNCATE TABLE {tgt_table}.d_retail_locn_t"
        db.execute_query(truncate_query)
        db.commit()
        print(f"Truncated table {tgt_table}.d_retail_locn_t")

        insert_query = f"""
        INSERT INTO {tgt_table}.d_retail_locn_t (LOCN_ID, RGN_KY, CNTRY_KY, LOCN_DESC, ROW_INSRT_TMS, ROW_UPDT_TMS)
        SELECT 
          S.ID AS LOCN_ID,                        
          R.RGN_KY,                    
          R.CNTRY_KY,                  
          S.STORE_DESC AS LOCN_DESC,   
          CURRENT_TIMESTAMP,           
          CURRENT_TIMESTAMP            
        FROM {tmp_table} S
        LEFT JOIN {tgt_table}.d_retail_rgn_t R ON S.REGION_ID = R.RGN_ID;
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