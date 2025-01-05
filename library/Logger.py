import logging
import os
import datetime
import json

from library.Variables import Variables


class Logger:
    def __init__(self, file_name):
        current_ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        self.file_name = f"{file_name}_{current_ts}.log"

        # Get log path and handle None gracefully
        log_path = Variables.get_variable('log_path')
        if not log_path:
            raise ValueError("Error: 'log_path' not found in configuration!")

        # Ensure log directory exists
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        self.log_path = os.path.join(log_path, self.file_name)
        self.logger = self.set_logger()

    def set_logger(self):
        logger = logging.getLogger(self.file_name)  # Use unique logger name for each instance
        logger.setLevel(logging.INFO)

        # Avoid duplicate handlers
        if not logger.handlers:
            file_handler = logging.FileHandler(self.log_path)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    def log_info(self, msg):
        self.logger.info(msg)

    def log_error(self, msg):
        self.logger.error(msg)

