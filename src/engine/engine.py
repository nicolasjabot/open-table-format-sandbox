from parser import SQLParser
from catalog import TableCatalog
from storage import Storage
from datetime import datetime
import os
from planner import Plan, SQLOperations


class Engine:
    def __init__(self):
        self.parser = SQLParser()
        self.catalog = TableCatalog()
        self.storage = Storage()
        self.dispatch = {
            SQLOperations.CREATE: self._create_operation,
            SQLOperations.INSERT: self._insert_operation,
            SQLOperations.SELECT: self._select_operation,
        }

    def query(self, sql: str):
        """First step of the Engine is to parse the query. 
        For that, we will create a query method and use the parser 
        class defined in parser.py"""
        plan = self.parser.parse_sql(sql)
        if not plan:
            print("Failed to parse SQL query.")
            return
        try:
            op_enum = SQLOperations[plan.op.upper()]
            handler = self.dispatch.get(op_enum)
            if handler:
                handler(plan)
            else:
                print(f"Unsupported operation: {plan.op}")
        except KeyError:
            print(f"Unknown operation: {plan.op}")

    def _create_operation(self, plan: Plan):
        try:
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


    def _insert_operation(self, plan:Plan):
        if not self.catalog.table_exists(plan.table):
                print(f"Table '{plan.table}' not found.")
                return

        if not plan.rows or any(len(row) != len(plan.columns) for row in plan.rows):
            print("Invalid INSERT statement: columns and values must match.")
            return

        # Convert rows to list of dicts
        rows = [dict(zip(plan.columns, row)) for row in plan.rows]

        file_path, timestamp = self.storage.insert_data(plan.table, rows)

        # Register the new file in the catalog
        self.catalog.add_file(plan.table, file_path, timestamp)

        print(f"Inserted {len(rows)} row(s) into '{plan.table}' at {file_path}")

    def _select_operation(self, plan:Plan):
        if not self.catalog.table_exists(plan.table):
            pass
        #         print(f"Table '{plan.table}' not found.")
        #         return

        #     rows = self.catalog[plan.table]["rows"]
        #     columns = plan.columns

        #     print("Query Result:")
        #     for row in rows:
        #         if columns == ["*"]:
        #             print(row)
        #         else:
        #             print({col: row.get(col) for col in columns})

   


# if __name__ == "__main__":
#     engine = Engine()
#     engine.query("SELECT * FROM test_table")