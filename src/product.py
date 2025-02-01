import os
from library.Database import Database
from library.Variables import Variables

file_name = os.path.basename(__file__).split('.')[0]

try:
    # Initialize the database connection
    db = Database(file_name)
    print("Connected to the database")

    # Export data from database to CSV
    df = db.ext_to_file(file_name)

    # Load data into staging table
    db.load_to_stg(file_name)

    db.delete_csv(f"C://ProgramData//MySQL//MySQL Server 8.0//Uploads//{file_name}.csv")


    # # SCD2 transformation: Merge staging into target
    # merge_query_scd2 = f"""
    # MERGE INTO DW_TGT.product TGT
    # USING DW_TMP.product TMP
    # ON TGT.product_id = TMP.product_id
    # WHEN MATCHED AND (TGT.product_name <> TMP.product_name OR TGT.product_price <> TMP.product_price)
    # THEN
    #     UPDATE SET
    #         TGT.eff_end_date = CURRENT_TIMESTAMP() - INTERVAL 1 DAY,
    #         TGT.active_flag = FALSE,
    #         TGT.rcd_upd_ts = CURRENT_TIMESTAMP()
    # WHEN NOT MATCHED
    # THEN
    #     INSERT (product_key, product_id, product_name, product_price, product_desc, rcd_ins_ts, rcd_upd_ts, eff_start_date, eff_end_date, active_flag)
    #     VALUES (
    #         GENERATE_PRODUCT_KEY(),
    #         TMP.product_id,
    #         TMP.product_name,
    #         TMP.product_price,
    #         TMP.product_desc,
    #         CURRENT_TIMESTAMP(),
    #         CURRENT_TIMESTAMP(),
    #         CURRENT_DATE(),
    #         '9999-12-31',
    #         TRUE
    #     );
    # """
    # db.execute_query(merge_query_scd2)
    # print("Data processed successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    try:
        db.disconnect()
        print("Database connection closed.")
    except Exception as e:
        print(f"Failed to disconnect: {e}")
