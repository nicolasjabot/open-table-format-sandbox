import pandas as pd
import os
from datetime import datetime

STORAGE_PATH = "src/storage/"

class Storage:
    """We are using this class is to communicate with the storage and the "actual" data"""
    def __init__(self, storage_path: str = STORAGE_PATH):
        self.storage_path = storage_path
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        self.catalog = self.write_parquet

    def write_parquet(self, table_name: str, data: list[dict]):
        """method for handling a CREATE operation"""
        print(f"Data to be written: {data}")  # Debugging the input data
        df = pd.DataFrame(data)
        print(f"DataFrame created:\n{df}")  # Debugging the DataFrame   
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(self.storage_path, table_name)
        os.makedirs(path, exist_ok=True)
        file_name = f"file1_{timestamp}.parquet"
        file_path = os.path.join(path, file_name)
        df.to_parquet(file_path, index=False)
        print(f"DataFrame written to {file_path}")  # Confirming the write operation
        return file_path, timestamp
    
    
    def insert_data(self, table_name: str, data: list[dict]):
        """method for handling a INSERT operation"""
        if not data:
            raise ValueError("No data provided for insertion.")
        table_path = os.path.join(self.storage_path, table_name)

        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name} does not exist. First CREATE table.")

        return self.write_parquet(table_name, data)


    def read_data(self, table_name: str) -> pd.DataFrame:
        """method for handling a SELECT opration"""
        table_path = os.path.join(self.storage_path, table_name)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name}' does not exist")
        
        parquet_files = [
            os.path.join(table_path, f)
            for f in os.listdir(table_path)
            if f.endswith(".parquet")
        ]

        if not parquet_files:
            raise FileNotFoundError (f"No data files found for table '{table_name}'")
        
        # sort files by date and select latest only
        df = max(parquet_files, key=os.path.getmtime)
        return pd.read_parquet(df)


### test snippet to check if the create the file in the storage ###

# if __name__ == "__main__":
#     # Test data
#     data = [
#         {"name": "Alice", "age": 30},
#         {"name": "Bob", "age": 25},
#         {"name": "Nico", "age": 27}
#     ]

#     storage = Storage()
#     file_path, timestamp = storage.write_parquet("test_table", data)
#     print(f"Data written to: {file_path} at {timestamp}")

# Corrected test snippet to read the data
if __name__ == "__main__":
    # Initialize the storage
    storage = Storage()
    
    # Define the table name
    table_name = "test_table"
    
    try:
        # Attempt to read data from storage
        df = storage.read_data(table_name)
        print(f"Data read from table '{table_name}':\n")
        print(df)
    except FileNotFoundError as e:
        print(f"Error: {e}")

# if __name__ == "__main__":
#     # Initialize the storage
#     storage = Storage()
    
#     # Define the table name
#     table_name = "test_table"
    
#     # Test data
#     data = [
#         {"name": "Alice", "age": 30},
#         {"name": "Bob", "age": 25},
#         {"name": "Nico", "age": 27}
#     ]
    
#     try:
#         # Write data to the table
#         file_path, timestamp = storage.write_parquet(table_name, data)
#         print(f"Data written to: {file_path} at {timestamp}")
        
#         # Verify the written file
#         df_check = pd.read_parquet(file_path)
#         print(f"Data read back from {file_path}:\n{df_check}")
        
#         # Read data from the table
#         df = storage.read_data(table_name)
#         print(f"Data read from table '{table_name}':\n{df}")
#     except FileNotFoundError as e:
#         print(f"Error: {e}")