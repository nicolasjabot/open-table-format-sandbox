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

    def query(self, sql: str):
        """Parse the query and delegate execution to the catalog"""
        plan = self.parser.parse_sql(sql)
        if not plan:
            print("Failed to parse SQL query.")
            return

        try:
            self.catalog.execute(plan)
        except Exception as e:
            print(f"Execution failed: {e}")

# class Engine:
#     def __init__(self):
#         self.parser = SQLParser()
#         self.catalog = TableCatalog()
#         self.storage = Storage()
#         self.dispatch = {
#             SQLOperations.CREATE: self._create_operation,
#             SQLOperations.INSERT: self._insert_operation,
#             SQLOperations.SELECT: self._select_operation,
#         }

#     # ------------------- Dispatch entrypoint ------------------
    
#     def query(self, sql: str):
#         """First step of the Engine is to parse the query. 
#         For that, we will create a query method and use the parser 
#         class defined in parser.py"""
#         plan = self.parser.parse_sql(sql)
#         if not plan:
#             print("Failed to parse SQL query.")
#             return
#         try:
#             op_enum = SQLOperations[plan.op.upper()]
#             handler = self.dispatch.get(op_enum)
#             if handler:
#                 handler(plan)
#             else:
#                 print(f"Unsupported operation: {plan.op}")
#         except KeyError:
#             print(f"Unknown operation: {plan.op}")
    
#     # ------------------- CRUD operations ----------------------
    
#     def _create_operation(self, plan: Plan):
#         try:
#             columns_with_type = [(col, "string") for col in plan.columns]
#             self.catalog.register_table(plan.table, columns_with_type)
#             print(f"Table '{plan.table}' created with columns: {plan.columns}")

#             path = os.path.join(self.storage.storage_path, plan.table) #move this to the catalog
#             os.makedirs(path, exist_ok=True)
#         except ValueError as e:
#             print(f" {e}")

#             print("DEBUG: Attempting to register table")
#             self.catalog.register_table(plan.table, columns_with_type)

#     def _insert_operation(self, plan:Plan):
#         if not self.catalog.table_exists(plan.table):
#                 print(f"Table '{plan.table}' not found.")
#                 return

#         if not plan.rows or any(len(row) != len(plan.columns) for row in plan.rows):
#             print("Invalid INSERT statement: columns and values must match.")
#             return

#         rows = [dict(zip(plan.columns, row)) for row in plan.rows]

#         file_path, timestamp = self.storage.insert_data(plan.table, rows)

#         self.catalog.add_file(plan.table, file_path, timestamp)

#         print(f"Inserted {len(rows)} row(s) into '{plan.table}' at {file_path}")

#     def _select_operation(self, plan:Plan):
#         if not self.catalog.table_exists(plan.table):
#             # pass
#             print(f"Table '{plan.table}' not found.")
#             return

#         try:
#             df = self.storage.read_data(plan.table)
#         except Exception as e:
#             print(f"Error reading data: {e}")
#             return
        
#         if plan.columns == ["*"]:
#             print(df.to_string(index=False))
#         else: 
#             missing = [col for col in plan.columns if col not in df.columns]
#             if missing:
#                 print(f"Columns not found: {missing}")
#                 return
#             print(df[plan.cloumns].to_string(index=False))
   

# if __name__ == "__main__":
#     engine = Engine()
#     engine.query("select * from test_table")