import os

from library.Database import Database
from library.Variables import Variables

file_name = os.path.basename(__file__).split('.')[0]
try:
    file_path = f"C://ProgramData//MySQL//MySQL Server 8.0//Uploads//{file_name}.csv"
    db= Database(file_name)
    print("Connected to the database")

    # df= db.ext_to_file(file_name)
    db.load_to_stg(file_name)
    print(f"Loaded from {file_path} to {file_name}")
    # db.delete_csv(f"C://ProgramData//MySQL//MySQL Server 8.0//Uploads//{file_name}.csv")

except Exception as e:
    print(f"An error : {e}")
finally:
    try:
        db.disconnect()
    except Exception as e:
        print(f"Failed to disconnect: {e}");