import pandas as pd
import pyarrow.parquet as pq
import os
from datetime import datetime

STORAGE_PATH = "src/storage/"

class Storage:
    """The goal of that class is to communicate with the storage and the "actual" data"""
    def __init__(self, storage_path: str = STORAGE_PATH):
        self.storage_path = storage_path
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        self.catalog = self.write_parquet

    def write_parquet(self, table_name: str, data: list[dict]):
        df = pd.DataFrame(data)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(self.storage_path, table_name)
        os.makedirs(path, exist_ok=True)
        file_name = f"file1_{timestamp}.parquet"
        file_path = os.path.join(path, file_name)
        df.to_parquet(file_path, index=False)
        return file_path, timestamp
    
    def insert_data(self, table_name: str, data: list[dict]):
        if not data:
            raise ValueError("No data provided for insertion.")
        table_path = os.path.join(self.storage_path, table_name)

        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name} does not exist. First CREATE table.")

        return self.write_parquet(table_name, data)

# def read_parquet(file_paths):
#     dfs = [pd.read_parquet(p) for p in file_paths]
#     return pd.concat(dfs)

### test snippet to check if the create the file in the storage ###

if __name__ == "__main__":
    # Test data
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Nico", "age": 27}
    ]

    storage = Storage()
    file_path, timestamp = storage.insert_data("test_table", data)
    print(f"Data written to: {file_path} at {timestamp}")