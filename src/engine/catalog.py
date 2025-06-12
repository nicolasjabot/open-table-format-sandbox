import json
import os
from datetime import datetime
from planner import Plan, SQLOperations
from storage import Storage


CATALOG_PATH = "src/catalog/manifest.json"
STORAGE_PATH = "src/storage/"

class TableCatalog:
    """
    the table class is used to interact between the engine and the metadata 
    in here, we communicate about the location and what should be executed on the storage
    in my case, it's a manifest.json file including schema, table, metadata information
    """
    def __init__(self, catalog_path: str = CATALOG_PATH, storage_path: str = STORAGE_PATH):
        self.catalog_path = catalog_path
        self.storage_path = storage_path
        self.storage = Storage()
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)

        self.dispatch = {
            SQLOperations.CREATE: self._create,
            SQLOperations.INSERT: self._insert,
            SQLOperations.SELECT: self._select,
        }

        self.catalog = self._load_catalog()

    # ------------------- Dispatch entrypoint ------------------
    def execute(self, plan: Plan):
        try:
            op_enum = SQLOperations[plan.op.upper()]
            handler = self.dispatch.get(op_enum)
            if not handler:
                raise ValueError(f"Unsupported operation {plan.op}")
            return handler(plan)
        except KeyError:
            raise ValueError(f"Unknown operation {plan.op}")

    # ------------------- CRUD operations ----------------------

    def _create(self, plan: Plan):
        table_name = plan.table
        columns = plan.columns

        if table_name in self.catalog["tables"]:
            raise ValueError(f"Table '{table_name}' already exists")

        schema = [[col[0], col[1]] for col in columns]  

        self.catalog["tables"][table_name] = {
            "schema": schema,
            "files": []  # list of {'path': str, 'timestamp': str}
        }
        self._save_catalog()

        #register table in storage
        path = os.path.join(self.storage.storage_path, plan.table) 
        os.makedirs(path, exist_ok=True)    

        print(f"Table '{table_name}' registered with schema: {schema}")

    def _insert(self, plan = Plan):
        table_name = plan.table

        if table_name not in self.catalog["tables"]:
            raise ValueError(f"Table '{table_name}' not found")
        
        if not plan.rows or any(len(row) != len(plan.columns) for row in plan.rows):
            raise ValueError("Invalid INSERT statement: columns and values must match.")

        rows = [dict(zip(plan.columns, row)) for row in plan.rows]
        schema = self.get_table_schema(table_name)

        file_path, timestamp = self.storage.write_parquet(table_name, rows, schema= schema)

        self.catalog["tables"][table_name]["files"].append({
            "path": file_path,
            "timestamp": timestamp
        })
        self._save_catalog()

        print(f"Inserted {len(rows)} rows into '{table_name}'")

    def _select(self, plan: Plan):
        table_name = plan.table

        if table_name not in self.catalog["tables"]:
            raise ValueError(f"Table '{table_name}' not found.")

        df = self.storage.read_data(table_name)

        if plan.columns == ["*"]:
            print(df.to_string(index=False))
        else:
            missing = [col for col in plan.columns if col not in df.columns]
            if missing:
                raise ValueError(f"Columns not found: {missing}")
            print(df[plan.columns].to_string(index=False))

    # ------------------- Catalog IO ----------------------    

    def _load_catalog(self):
        if not os.path.exists(self.catalog_path):
            return {"tables": {}}
        
        with open(self.catalog_path, "r") as f:
            return json.load(f)

    def _save_catalog(self):
        with open(self.catalog_path, "w") as f:
            json.dump(self.catalog, f, indent=2)

    # ------------------- Helper methods ----------------------

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
