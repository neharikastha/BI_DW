import mysql.connector
from mysql.connector import Error
import json
from library.Logger import Logger  # Assuming Logger is in the 'library' folder


class CsvToDatabaseLoader:
    def __init__(self, config_path, logger):
        """
        Initialize the CsvToDatabaseLoader class with the configuration file path and logger.

        :param config_path: Path to the JSON configuration file.
        :param logger: Logger instance for logging messages.
        """
        self.config_path = config_path
        self.logger = logger
        self.db_config = self.load_config()

    def load_config(self):
        """
        Load the database configuration from the JSON file.

        :return: Database configuration dictionary.
        """
        try:
            with open(self.config_path, "r") as file:
                config = json.load(file)
            self.logger.log_info("Configuration loaded successfully.")
            return config["database"]
        except FileNotFoundError:
            self.logger.log_error(f"Configuration file not found at {self.config_path}.")
            return None
        except json.JSONDecodeError as e:
            self.logger.log_error(f"Error parsing JSON configuration file: {e}")
            return None

    def load_csv_to_staging_and_target(self, csv_path, staging_table, source_table):
        """
        Load data from a CSV file into the staging table and insert data into the source table.

        :param csv_path: Path to the CSV file.
        :param staging_table: Name of the staging table.
        :param source_table: Name of the source table.
        """
        if not self.db_config:
            self.logger.log_error("Database configuration is not loaded. Exiting.")
            return

        # Attempt to connect to the database
        try:
            connection = mysql.connector.connect(**self.db_config, allow_local_infile=True)
            if connection.is_connected():
                self.logger.log_info("Successfully connected to the database.")
                cursor = connection.cursor()

                # Load the data from CSV into the staging table
                load_query = f"""
                LOAD DATA LOCAL INFILE '{csv_path}'
                INTO TABLE `{staging_table}`
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES;
                """
                cursor.execute(load_query)
                connection.commit()
                self.logger.log_info(f"Data successfully loaded into {staging_table}.")

                # Insert data from staging table to source table
                insert_query = f"""
                INSERT INTO `{source_table}`
                SELECT * FROM `{staging_table}`
                ON DUPLICATE KEY UPDATE
                    productname = VALUES(productname),
                    price = VALUES(price),
                    stockquantity = VALUES(stockquantity);
                """
                cursor.execute(insert_query)
                connection.commit()
                self.logger.log_info(f"Data successfully inserted into {source_table}.")

        except Error as e:
            self.logger.log_error(f"Database operation failed: {e}")
        finally:
            # Close the connection
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
                self.logger.log_info("Database connection closed.")


# Example usage
if __name__ == "__main__":
    # Path to the configuration file
    config_path = "C:/Users/nehar/PycharmProjects/ETL_Project/config/config.json"

    # Path to the CSV file
    csv_file_path = "C:/Users/nehar/PycharmProjects/ETL_Project/output/product_data.csv"

    # Table names
    staging_table = "OLAP_Product"
    source_table = "OLTP_Product"

    # Initialize the logger
    log = Logger("CsvToDatabaseLoader")

    # Create an instance of CsvToDatabaseLoader
    loader = CsvToDatabaseLoader(config_path, log)

    # Load CSV into the staging and target tables
    loader.load_csv_to_staging_and_target(csv_file_path, staging_table, source_table)
