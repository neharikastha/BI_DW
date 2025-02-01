from library.Database import Database
from library.Variables import Variables
import os

file_name = os.path.basename(__file__).split('.')[0]

def load_tmp_to_tgt():
    try:
        db = Database(file_name)
        print("Connected to the database")

        stg_table = f"{Variables.get_variable('STG_DB')}.stg_sales"
        tmp_table = f"{Variables.get_variable('TMP_DB')}.tmp_sales"
        tgt_table = f"{Variables.get_variable('TGT_DB')}.f_retail_sls_t"
        product = f"{Variables.get_variable('TGT_DB')}.D_RETAIL_PDT_T"
        customer = f"{Variables.get_variable('TGT_DB')}.D_RETAIL_CUSTOMER_T"
        locn = f"{Variables.get_variable('TGT_DB')}.D_RETAIL_LOCN_T"
        dim_calendar = f"{Variables.get_variable('TGT_DB')}.dim_calendar"

        # Disable Foreign Key Checks
        db.execute_query("SET FOREIGN_KEY_CHECKS = 0;")

        # Truncate tmp_table before inserting new data
        db.execute_query(f"TRUNCATE TABLE {tmp_table};")
        db.commit()
        print(f"Truncated table {tmp_table}")

        # Insert data from staging to temp table
        insert_query = f"""
        INSERT INTO {tmp_table} (ID, STORE_ID, PRODUCT_ID, CUSTOMER_ID, TRANSACTION_TIME, QUANTITY, AMOUNT, DISCOUNT)
        SELECT ID, STORE_ID, PRODUCT_ID, CUSTOMER_ID, TRANSACTION_TIME, QUANTITY, AMOUNT, DISCOUNT
        FROM {stg_table}
        """
        db.execute_query(insert_query)
        db.commit()
        print(f"Data loaded from {stg_table} to {tmp_table}")

        # Truncate target table before inserting new data
        db.execute_query(f"TRUNCATE TABLE {tgt_table};")
        db.commit()
        print(f"Truncated table {tgt_table}")

        # Insert data from tmp_table to tgt_table with correct joins
        insert_query = f"""
        INSERT INTO {tgt_table} (SLS_ID, LOCN_KY, DT_KY, PDT_KY, CUSTOMER_KY, TRANSACTION_TIME, QTY, AMT, DSCNT, ROW_INSRT_TMS, ROW_UPDT_TMS)
        SELECT 
          S.ID,            
          L.LOCN_KY,
          CA.DAY_KEY,
          P.PDT_KY,                  
          C.CUSTOMER_KY,             
          S.TRANSACTION_TIME,        
          S.QUANTITY AS QTY,         
          S.AMOUNT AS AMT,           
          S.DISCOUNT AS DSCNT,       
          CURRENT_TIMESTAMP,         
          CURRENT_TIMESTAMP          
        FROM {tmp_table} S
        LEFT JOIN {product} P ON S.PRODUCT_ID = P.PDT_ID  
        LEFT JOIN {customer} C ON S.CUSTOMER_ID = C.CUSTOMER_ID
        LEFT JOIN {locn} L ON S.STORE_ID = L.LOCN_ID
        LEFT JOIN {dim_calendar} CA ON DATE(S.TRANSACTION_TIME) = CA.DATE;
        """
        db.execute_query(insert_query)
        db.commit()
        print(f"Data loaded from {tmp_table} to {tgt_table}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        try:
            # Restore Foreign Key Checks
            db.execute_query("SET FOREIGN_KEY_CHECKS = 1;")
            db.disconnect()
        except Exception as e:
            print(f"Failed to disconnect: {e}")

if __name__ == "__main__":
    load_tmp_to_tgt()
