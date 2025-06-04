from parser import SQLParser, Plan
from catalog import TableCatalog
from storage import Storage
from datetime import datetime
import os

class Engine:
    def __init__(self):
        self.parser = SQLParser()
        self.catalog = TableCatalog()
        self.storage = Storage()

    def query(self, sql: str):
        plan = self.parser.parse_sql(sql)

        if plan.op == "CREATE":
            try:
                # Register table in metadata
                columns_with_type = [(col, "string") for col in plan.columns]
                self.catalog.register_table(plan.table, columns_with_type)
                print(f"Table '{plan.table}' created with columns: {plan.columns}")

                # create storage folder
                path = os.path.join(self.storage.storage_path, plan.table)
                os.makedirs(path, exist_ok=True)
                self.storage.write_parquet(plan.table, columns_with_type)
            except ValueError as e:
                print(f" {e}")

                print("DEBUG: Attempting to register table")
                self.catalog.register_table(plan.table, columns_with_type)

        elif plan.op == "INSERT":
            if plan.table not in self.catalog:
                print(f"Table '{plan.table}' not found.")
                return

            if not hasattr(plan, "values") or len(plan.columns) != len(plan.values):
                print("Invalid INSERT statement: columns and values must match.")
                return

            row = {col: val for col, val in zip(plan.columns, plan.values)}
            self.catalog[plan.table]["rows"].append(row)

            print(f"Inserted row into '{plan.table}': {row}")


        elif plan.op == "SELECT":
            if plan.table not in self.catalog:
                print(f"Table '{plan.table}' not found.")
                return

            rows = self.catalog[plan.table]["rows"]
            columns = plan.columns

            print("Query Result:")
            for row in rows:
                if columns == ["*"]:
                    print(row)
                else:
                    print({col: row.get(col) for col in columns})

        else:
            print(f"Unsupported operation: {plan.op}")