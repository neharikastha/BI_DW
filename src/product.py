import os

from library.Database import Database
from library.Variables import Variables

file_name = os.path.basename(__file__).split('.')[0]
try:
    db= Database(file_name)
    print("Connected to the database")

    df= db.ext_to_file(file_name)
    stg_table = db.load_to_stg(file_name)

except Exception as e:
    print(f"An error : {e}")
finally:
    try:
        db.disconnect()
    except Exception as e:
        print(f"Failed to disconnect: {e}");