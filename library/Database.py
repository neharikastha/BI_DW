import os
import mysql.connector
import pandas as pd
from library.Variables import Variables
from library.Logger import Logger

class Database:
    def __init__(self, file_name):
        try:
            self.logger = Logger(file_name)
            self.connection = mysql.connector.connect(
                host=Variables.get_variable("host"),
                port=Variables.get_variable("port"),
                user=Variables.get_variable("user"),
                password=Variables.get_variable("password"),
                allow_local_infile=True  # Enable LOCAL INFILE for CSV loading
            )
            self.cursor = self.connection.cursor()
            if self.connection.is_connected():
                self.logger.log_info("Successfully connected to MySQL!")
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error connecting to MySQL: {e}")
            raise  # Re-raise the exception if connection fails

    def execute_query(self, select_query):
        """Executes a given SELECT query."""
        self.logger.log_info(select_query)
        try:
            self.cursor.execute(select_query)
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error executing query: {e}")
            raise

    def fetchall(self):
        """Fetches all records from the last executed query."""
        try:
            results = self.cursor.fetchall()
            self.logger.log_info("Fetched all results.")
            return results
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error fetching results: {e}")
            raise

    def ext_to_file(self, table_name):
        """Exports data from the table into a CSV file."""
        file_path = f"C://ProgramData//MySQL//MySQL Server 8.0//Uploads//{table_name}.csv"
        select_query = f"SELECT * FROM {Variables.get_variable('SRC_DB')}.{table_name}"

        self.execute_query(select_query)
        data = self.fetchall()

        # Get column names from the cursor and create a pandas DataFrame
        columns = [desc[0] for desc in self.cursor.description]
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(file_path, index=False)
        self.logger.log_info(f"Data exported to {file_path}")
        return df

    def load_to_stg(self, file_name):
        try:
            file_path = f"C://ProgramData//MySQL//MySQL Server 8.0//Uploads//{file_name}.csv"

            # # Read CSV and handle empty values
            # df = pd.read_csv(file_path, dtype=str)  # Read all as string to handle empty cases
            # df['customer_id'] = df['customer_id'].replace({"": None, "NULL": None, "null": None, " ": None})
            # df.to_csv(file_path, index=False, na_rep='NULL')  # Save with 'NULL' representation
            # print(pd.read_csv(file_path).head())  # Check if "NULL" appears correctly

            # Load cleaned file into MySQL
            load_query = f"""
            LOAD DATA LOCAL INFILE '{file_path}'
            INTO TABLE OLAP_Neharika_Stage.stg_sales
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS
             (ID, STORE_ID, PRODUCT_ID, @CUSTOMER_ID, TRANSACTION_TIME, QUANTITY, AMOUNT, @DISCOUNT)
             SET 
                 CUSTOMER_ID = NULLIF(@CUSTOMER_ID, ''),
                 DISCOUNT = NULLIF(@DISCOUNT, '');

            """

            self.cursor.execute(load_query)
            self.connection.commit()

            # Ensure empty values are NULL in MySQL
            update_query = f"""
            UPDATE OLAP_Neharika_Stage.stg_{file_name}
            SET CUSTOMER_ID = NULL
            WHERE CUSTOMER_ID = '' OR CUSTOMER_ID IS NULL;
            """
            self.cursor.execute(update_query)
            self.connection.commit()

            self.logger.log_info(f"Data loaded successfully into the staging table stg_{file_name}.")

        except mysql.connector.Error as e:
            self.logger.log_error(f"Error loading data: {e}")

    # def bulk_insert(self, table_name, headers, batch_data):
    #     try:
    #         placeholders = ", ".join(["%s"] * len(headers))
    #
    #         insert_query = f"""
    #             INSERT INTO {table_name} ({', '.join(headers)})
    #             VALUES ({placeholders})
    #         """
    #
    #         cleaned_batch_data = [
    #             [None if value in ["", "NULL", "null", " "] else value for value in row] for row in batch_data
    #         ]
    #
    #         self.cursor.executemany(insert_query, cleaned_batch_data)
    #         self.commit()
    #         self.logger.log_info(f"Successfully inserted {len(batch_data)} records into {table_name}.")
    #     except mysql.connector.Error as e:
    #         self.logger.log_error(f"Error inserting data into {table_name}: {e}")

    def commit(self):
        """Commits the current transaction."""
        try:
            self.connection.commit()
            self.logger.log_info("Transaction committed successfully.")
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error committing transaction: {e}")
            raise

    def delete_csv(self, file_path):
        """Deletes the CSV file after loading."""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                self.logger.log_info(f"CSV file {file_path} deleted successfully.")
            else:
                self.logger.log_error(f"File {file_path} does not exist.")
        except Exception as e:
            self.logger.log_error(f"Error deleting CSV file: {e}")
            raise

    def disconnect(self):
        """Closes the database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            self.logger.log_info("Connection closed.")
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error closing connection: {e}")
            raise



