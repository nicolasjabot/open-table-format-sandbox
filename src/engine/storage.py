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

    def write_parquet(self, table_name: str, data: list[dict], schema: list[tuple] = None):
        """method for handling a CREATE operation"""
        # Ensure data is  list of dicts
        if not isinstance(data, list) or not all(isinstance(row, dict) for row in data):
            raise ValueError("Data must be a list of dictionaries (rows).")
        # detect and block accidental schema being passed as data
        if all(isinstance(row, tuple) and len(row) == 2 for row in data):
            raise ValueError("It looks like you're passing schema tuples instead of row data.")

        df = pd.DataFrame(data)

        if schema:
            expected_cols = [col[0] for col in schema]
            if sorted(df.columns) != sorted(expected_cols):
                raise ValueError(f"Data columns {list(df.columns)} do not match expected schema {expected_cols}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(self.storage_path, table_name)
        os.makedirs(path, exist_ok=True)
        file_name = f"file1_{timestamp}.parquet"
        file_path = os.path.join(path, file_name)
        df.to_parquet(file_path, index=False)

        return file_path, timestamp
    
    
    def insert_data(self, table_name: str, data: list[dict]):
        """method for handling a INSERT operation.
            Appends new data to existing table and writes it as a new file."""
        if not data:
            raise ValueError("No data provided for insertion.")
        table_path = os.path.join(self.storage_path, table_name)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name} does not exist. First CREATE table.")
        
        new_df = pd.DataFrame(data)

        # read existing data if it exists
        existing_files = [
            os.path.join(table_path, f)
            for f in os.listdir(table_path)
            if f.endswith(".parquet")
        ]

        if existing_files:
            existing_dfs = [pd.read_parquet(f) for f in existing_files]
            existing_df = pd.concat(existing_dfs, ignore_index=True)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df

        # Step 2: Write combined DataFrame to a new file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"file_{timestamp}.parquet"
        file_path = os.path.join(table_path, file_name)
        combined_df.to_parquet(file_path, index=False)

        return file_path, timestamp

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


# # test snippet to read the data
# if __name__ == "__main__":
#     storage = Storage()
    
#     table_name = "test_table"
    
#     try:
#         df = storage.read_data(table_name)
#         print(f"Data read from table '{table_name}':\n")
#         print(df)
#     except FileNotFoundError as e:
#         print(f"Error: {e}")
