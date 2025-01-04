import mysql.connector
from mysql.connector import Error
import json
import csv
from library.Logger import Logger  # Assuming Logger is in a folder named 'library'


class ExportToCsv:
    def __init__(self, config_path, logger):
        """
        Initialize the ExportToCsv class with the configuration file path and logger.

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

    def fetch_and_export(self, query, output_csv_path):
        """
        Fetch data from the database using the given query and export it to a CSV file.

        :param query: SQL query to fetch data.
        :param output_csv_path: Path to save the CSV file.
        """
        if not self.db_config:
            self.logger.log_error("Database configuration is not loaded. Exiting.")
            return

        # Attempt to connect to the database
        try:
            connection = mysql.connector.connect(**self.db_config)
            if connection.is_connected():
                self.logger.log_info("Successfully connected to the database.")
                cursor = connection.cursor()

                # Execute the query
                cursor.execute(query)
                results = cursor.fetchall()
                self.logger.log_info("Query executed successfully. Results fetched.")

                # Get column names from the cursor description
                column_names = [desc[0] for desc in cursor.description]

                # Write the results to a CSV file
                with open(output_csv_path, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)

                    # Write the header (column names)
                    writer.writerow(column_names)

                    # Write the data rows
                    writer.writerows(results)

                self.logger.log_info(f"Data successfully written to {output_csv_path}.")

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

    # Output CSV file path
    output_csv_path = "C:/Users/nehar/PycharmProjects/ETL_Project/output/product_data.csv"

    # Initialize the logger
    log = Logger("ExportToCsv")

    # Create an instance of ExportToCsv
    exporter = ExportToCsv(config_path, log)

    # Define the query and call the fetch_and_export method
    sample_query = "SELECT * FROM OLTP_Product"
    exporter.fetch_and_export(sample_query, output_csv_path)
