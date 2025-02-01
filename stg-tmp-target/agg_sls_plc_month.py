# from library.Database import Database
# from library.Variables import Variables
# import os
#
# file_name = os.path.basename(__file__).split('.')[0]
#
# def load_stg_to_tmp():
#     try:
#         db = Database(file_name)
#         print("Connected to the database")
#
#         # Database and table names
#         stg_customer_table = f"{Variables.get_variable('STG_DB')}.stg_customer"
#         stg_product_table = f"{Variables.get_variable('STG_DB')}.stg_product"
#         stg_store_table = f"{Variables.get_variable('STG_DB')}.stg_store"
#         sales_table = f"{Variables.get_variable('STG_DB')}.stg_sales"  # Assuming sales table exists in the stage schema
#         tmp_table = f"{Variables.get_variable('TMP_DB')}.tmp_agg_sls_plc_month"
#
#         # Truncate the tmp_table before inserting new data
#         truncate_query = f"TRUNCATE TABLE {tmp_table}"
#         db.execute_query(truncate_query)
#         db.commit()
#         print(f"Truncated table {tmp_table}")
#
#         # Insert data into tmp_table from the staging tables and sales data with LEFT JOIN
#         insert_query = f"""
#         INSERT INTO {tmp_table} (PDT_KY, STORE_KY, CUSTOMER_KY, TOTAL_QTY, TOTAL_AMT, TOTAL_DSCNT)
#         SELECT
#             s.PRODUCT_ID AS PDT_KY,
#             s.STORE_ID AS STORE_KY,
#             c.ID AS CUSTOMER_KY,
#             s.Quantity AS TOTAL_QTY,
#             s.Amount AS TOTAL_AMT,
#             s.Discount AS TOTAL_DSCNT
#         FROM
#             {sales_table} s
#             left join
#             {stg_customer_table} c ON s.ID = c.ID  -- Correct column names for sales table
#         """
#
#         db.execute_query(insert_query)
#         db.commit()
#         print(f"Data loaded from staging tables into {tmp_table}")
#
#     except Exception as e:
#         print(f"An error occurred: {e}")
#     finally:
#         try:
#             db.disconnect()
#         except Exception as e:
#             print(f"Failed to disconnect: {e}")
#
# if __name__ == "__main__":
#     load_stg_to_tmp()
