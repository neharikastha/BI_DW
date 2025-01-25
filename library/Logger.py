import os
import logging
import datetime
import json
from library.Variables import Variables


# Function to load and read the config.json file
def load_config(config_path):
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at {config_path}")

        with open(config_path, 'r') as file:
            config_data = json.load(file)

        # Ensure log_path exists in the configuration
        if 'log_path' not in config_data:
            raise ValueError("Missing 'log_path' in configuration.")

        return config_data
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None
    except ValueError as e:
        print(f"Error: {e}")
        return None


class Logger:
    def __init__(self, file_name):
        current_ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Load config file to get log_path dynamically
        config_path = "C:/Users/nehar/ETLProject/config/config.json"  # Adjust the path as needed
        config = load_config(config_path)

        if not config:
            raise ValueError("Configuration could not be loaded.")

        # Get the base log path from the config
        log_path = config['log_path']

        # Ensure the base log path exists
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        # Append the Python script name to create a directory specific to the script
        log_dir = os.path.join(log_path, file_name)

        # Create the directory for the specific script if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Create the full path for the log file, including the timestamp
        self.file_name = f"{file_name}_{current_ts}.log"
        self.log_path = os.path.join(log_dir, self.file_name)

        # Initialize logger
        self.logger = logging.getLogger(self.file_name)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler
        file_handler = logging.FileHandler(self.log_path)

        # Set up the formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(file_handler)

        # Debugging log path
        print(f"Log file created at: {self.log_path}")

    def log_info(self, msg):
        self.logger.info(msg)

    def log_error(self, msg):
        self.logger.error(msg)


# import os
# import logging
# import datetime
# from library.Variables import Variables
#
#
# class Logger:
#     def __init__(self, file_name):
#         current_ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
#
#         # Append the timestamp and filename to create a unique log file name
#         self.file_name = f"{file_name}_{current_ts}.log"
#
#         # Fetch the log path from environment variables or config
#         log_path = Variables.get_variable("log_path")
#         if not log_path:
#             raise ValueError("Error: 'log_path' not found in configuration!")
#
#         # Ensure the directory exists
#         if not os.path.exists(log_path):
#             os.makedirs(log_path)
#
#         # Create the complete log file path
#         self.log_path = os.path.join(log_path, self.file_name)
#
#         # Setting up the logger
#         self.logger = logging.getLogger(self.file_name)
#         self.logger.setLevel(logging.DEBUG)
#
#         # Create a file handler to write logs to the file
#         file_handler = logging.FileHandler(self.log_path)
#
#         # Set up a formatter for the log messages
#         formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#         file_handler.setFormatter(formatter)
#
#         # Add the handler to the logger
#         self.logger.addHandler(file_handler)
#
#         # Debugging log path to ensure it's correct
#         print(f"Log file created at: {self.log_path}")
#
#     def log_info(self, msg):
#         self.logger.info(msg)
#
#     def log_error(self, msg):
#         self.logger.error(msg)

# import logging
# import os
# import datetime
# import json
#
# from library.Variables import Variables
#
#
# class Logger:
#     def __init__(self, file_name):
#         current_ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
#         self.file_name = f"{file_name}_{current_ts}.log"
#
#         # Get log path and handle None gracefully
#         log_path = Variables.get_variable('log_path')
#         if not log_path:
#             raise ValueError("Error: 'log_path' not found in configuration!")
#
#         # Ensure log directory exists
#         if not os.path.exists(log_path):
#             os.makedirs(log_path)
#
#         self.log_path = os.path.join(log_path, self.file_name)
#         self.logger = self.set_logger()
#
#     def set_logger(self):
#         logger = logging.getLogger(self.file_name)  # Use unique logger name for each instance
#         logger.setLevel(logging.INFO)
#
#         # Avoid duplicate handlers
#         if not logger.handlers:
#             file_handler = logging.FileHandler(self.log_path)
#             formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#             file_handler.setFormatter(formatter)
#             logger.addHandler(file_handler)
#
#         return logger
#
#     def log_info(self, msg):
#         self.logger.info(msg)
#
#     def log_error(self, msg):
#         self.logger.error(msg)
#
