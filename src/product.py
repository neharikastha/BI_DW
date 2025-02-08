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

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    try:
        db.disconnect()
        print("Database connection closed.")
    except Exception as e:
        print(f"Failed to disconnect: {e}")
