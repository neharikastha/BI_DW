from library.Database import Database
from library.Variables import Variables
import os

file_name = os.path.basename(__file__).split('.')[0]

def load_stg_to_tmp():
    try:
        db = Database(file_name)
        print("Connected to the database")

        # Select the target database explicitly
        db.execute_query(f"USE {Variables.get_variable('TGT_DB')}")  # Assuming TGT_DB is the name of the target database
        db.commit()

        # Table names with dynamic DB name injection
        stg_table = f"{Variables.get_variable('STG_DB')}.stg_product"
        tmp_table = f"{Variables.get_variable('TMP_DB')}.tmp_product"
        tgt_table = f"{Variables.get_variable('TGT_DB')}"

        db.execute_query("SET FOREIGN_KEY_CHECKS = 0;")

        # Truncate the tmp_table before inserting new data
        truncate_query = f"TRUNCATE TABLE {tmp_table}"
        db.execute_query(truncate_query)
        db.commit()
        print(f"Truncated table {tmp_table}")

        # Insert data from staging table to temp table
        insert_query = f"""
        INSERT INTO {tmp_table} (ID, SUBCATEGORY_ID, PRODUCT_DESC)
        SELECT p.ID, p.SUBCATEGORY_ID, p.PRODUCT_DESC
        FROM {stg_table} p
        """
        db.execute_query(insert_query)
        db.commit()
        print(f"Data loaded from {stg_table} to {tmp_table}")



        truncate_query = f"TRUNCATE TABLE {tgt_table}.d_retail_pdt_t"
        db.execute_query(truncate_query)
        db.commit()
        print(f"Truncated table {tgt_table}.d_retail_pdt_t")

        insert_query = f"""
        INSERT INTO {tgt_table}.d_retail_pdt_t (PDT_ID,SUB_CTGRY_KY,CTGRY_KY,PDT_DESC, ROW_INSRT_TMS, ROW_UPDT_TMS)
            SELECT 
                P.ID,
                S.SUB_CTGRY_KY,        
                S.CTGRY_KY,
                P.PRODUCT_DESC,
                CURRENT_TIMESTAMP, 
                CURRENT_TIMESTAMP
        FROM {tmp_table} P
        LEFT JOIN {tgt_table}.d_retail_sub_ctgry_t S 
          ON P.SUBCATEGORY_ID = S.SUB_CTGRY_KY;
        """

        db.execute_query(insert_query)
        db.commit()
        print(f"Data loaded from {tmp_table} to {tgt_table}.d_retail_pdt_t")

        # # SCD1 Update - Update existing records in target table based on product_desc
        # update_query_scd1 = f"""
        # UPDATE {tgt_table} TGT
        # JOIN {tmp_table} TMP
        # ON TGT.pdt_id= TMP.pdt_id
        # SET
        #     TGT.pdt_desc = TMP.pdt_desc,
        #     TGT.ROW_UPDT_TMS = CURRENT_TIMESTAMP()
        # WHERE TGT.pdt_desc <> TMP.pdt_desc
        # """
        # db.execute_query(update_query_scd1)
        # db.commit()
        # print(f"Updated records in {tgt_table} from {tmp_table} for SCD1")
        #
        # # SCD1 Insert - Insert new records into the target table if product_desc does not exist
        # insert_query_scd1 = f"""
        # INSERT INTO {tgt_table} (pdt_ky, pdt_id, pdt_desc, ROW_INSRT_TMS, ROW_UPDT_TMS)
        # SELECT
        #     SHA2(CONCAT(TMP.pdt_id, TMP.pdt_desc), 256),  -- Generates SHA-256 hash
        #     TMP.pdt_id,
        #     TMP.pdt_desc,
        #     CURRENT_TIMESTAMP(),
        #     CURRENT_TIMESTAMP()
        # FROM {tmp_table} TMP
        # WHERE TMP.pdt_id NOT IN (SELECT pdt_id FROM {tgt_table})
        # """
        #
        # db.execute_query(insert_query_scd1)
        # db.commit()
        # print(f"Inserted new records into {tgt_table} from {tmp_table} for SCD1")
        #
        # # SCD2 Update - Update the end date and flag for old records based on product_desc
        # update_query_scd2 = f"""
        # UPDATE {tgt_table} TGT
        # JOIN {tmp_table} TMP
        # ON TGT.pdt_id = TMP.pdt_id
        # SET
        #     TGT.eff_end_date = CURRENT_TIMESTAMP() - INTERVAL 1 DAY,
        #     TGT.active_flag = FALSE,
        #     TGT.ROW_UPDT_TMS = CURRENT_TIMESTAMP()
        # WHERE TGT.pdt_desc <> TMP.pdt_desc
        # """
        # db.execute_query(update_query_scd2)
        # db.commit()
        # print(f"Updated end date and flags for old records in {tgt_table} for SCD2")
        #
        # # SCD2 Insert - Insert new records with new effective dates for the changed product_desc
        # insert_query_scd2 = f"""
        # INSERT INTO {tgt_table} (pdt_ky, pdt_id, pdt_desc, ROW_INSRT_TMS, ROW_UPDT_TMS, eff_start_date, eff_end_date, active_flag)
        # SELECT
        #     generate_pdt_ky(),
        #     TMP.pdt_id,
        #     TMP.pdt_desc,
        #     CURRENT_TIMESTAMP(),
        #     CURRENT_TIMESTAMP(),
        #     CURRENT_DATE(),
        #     '9999-12-31',
        #     TRUE
        # FROM {tmp_table} TMP
        # WHERE TMP.product_id NOT IN (SELECT pdt_id FROM {tgt_table})
        # """
        # db.execute_query(insert_query_scd2)
        # db.commit()
        # print(f"Inserted new active records into {tgt_table} from {tmp_table} for SCD2")

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
