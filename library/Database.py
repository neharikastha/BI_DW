import mysql.connector
from mysql.connector import Error
import json
from library.Logger import Logger  # Import your updated Logger class

def fetch_data_from_table(query):
    # Initialize the Logger
    logger = Logger(file_name="database_operations")

    # Path to the JSON configuration file
    config_path = "C:/Users/nehar/PycharmProjects/ETL_Project/config/config.json"

    # Read configuration from JSON file
    try:
        with open(config_path, "r") as file:
            config = json.load(file)
        db_config = config["database"]
        logger.log_info("Configuration file loaded successfully.")
    except FileNotFoundError:
        logger.log_error(f"Configuration file not found at {config_path}.")
        return
    except json.JSONDecodeError as e:
        logger.log_error(f"Error parsing JSON configuration file: {e}")
        return

    # Attempt to connect to the database
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            logger.log_info("Successfully connected to the database.")
            cursor = connection.cursor()

            # Execute the query
            cursor.execute(query)
            results = cursor.fetchall()
            logger.log_info("Query executed successfully. Results:")

            # Log each row of the result
            for row in results:
                logger.log_info(str(row))  # Convert the row to string for logging

    except Error as e:
        logger.log_error(f"Database operation failed: {e}")
    finally:
        # Close the connection
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            logger.log_info("Database connection closed.")

# Example usage
if __name__ == "__main__":
    # Replace 'OLTP_Product' with your table name
    sample_query = "SELECT * FROM OLTP_Product LIMIT 6"
    fetch_data_from_table(sample_query)
