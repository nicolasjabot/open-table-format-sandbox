import json
import os
from datetime import datetime

CATALOG_PATH = "src/catalog/manifest.json"


class TableCatalog:
    """the table class is used to interact between the engine and the metadata 
    in here, we communicate about the location and what should be executed on the storage
    in my case, it's a manifest.json file including schema, table and column information
    from there, the engine will understand what to do on the storage location"""
    def __init__(self, catalog_path: str = CATALOG_PATH):
        self.catalog_path = catalog_path
        os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
        self.catalog = self._load_catalog()

    def _load_catalog(self):
        if not os.path.exists(self.catalog_path):
            return {"tables": {}}
        
        with open(self.catalog_path, "r") as f:
            return json.load(f)

    def _save_catalog(self):
        with open(self.catalog_path, "w") as f:
            json.dump(self.catalog, f, indent=2)

    def register_table(self, table_name: str, columns: list[tuple]):
        if table_name in self.catalog["tables"]:
            raise ValueError(f"Table '{table_name}' already exists.")

        schema = [[col[0], col[1]] for col in columns]

        self.catalog["tables"][table_name] = {
            "schema": schema,
            "files": []  # list of {'path': str, 'timestamp': str}
        }
        self._save_catalog()

    def add_file(self, table_name: str, file_path: str, timestamp: str):
        if table_name not in self.catalog["tables"]:
            raise ValueError(f"Table '{table_name}' not found.")

        self.catalog["tables"][table_name]["files"].append({
            "path": file_path,
            "timestamp": timestamp
        })
        self._save_catalog()

    def get_table_schema(self, table_name: str):
        if table_name not in self.catalog["tables"]:
            raise ValueError(f"Table '{table_name}' not found.")
        return self.catalog["tables"][table_name]["schema"]

    def list_table_files(self, table_name: str):
        if table_name not in self.catalog["tables"]:
            raise ValueError(f"Table '{table_name}' not found.")
        return self.catalog["tables"][table_name]["files"]

    def list_tables(self):
        return list(self.catalog["tables"].keys())
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the catalog.
        """
        return table_name in self.catalog["tables"]
