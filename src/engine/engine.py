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
            SQLOperations.CREATE: self._parse_create,
            SQLOperations.INSERT: self._parse_insert,
            SQLOperations.SELECT: self._parse_select,
        }

    def query(self, sql: str):
        """First step of the Engine is to parse the query. 
        For that, we will create a query method and use the parser 
        class define in parser.py"""
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

    def _create(self, plan: Plan):
        pass

    def _insert(self, plan:Plan):
        pass

    def _select(self, plan:Plan):
        pass

   


# if __name__ == "__main__":
#     engine = Engine()
#     engine.query("SELECT * FROM test_table")