from sqlglot import parse
from planner import Plan, SQLOperations
from typing import Optional

class SQLParser:
    def __init__(self, enforce_engine='iceberg'):
        self.enforce_engine = enforce_engine
        self.dispatch = {
            SQLOperations.CREATE: self._parse_create,
            SQLOperations.INSERT: self._parse_insert,
            SQLOperations.SELECT: self._parse_select,
        }

    def parse_sql(self, query: str) -> Optional[Plan]:
        try:
            parsed = parse(query)[0] #only take the first element of the list of statements
            op = parsed.key.upper()
            op_enum = SQLOperations[op]
            return self.dispatch[op_enum](parsed)
        
        except Exception as e:
            print(f"Error parsing SQL: {e}")
            return None
        
    def _parse_create(self, parsed):
        """Handles CREATE table operation"""
        schema_expr = parsed.args["this"]
        table = str(schema_expr.this)    
        columns = [(col.this.name, col.args["kind"].this.value) for col in schema_expr.expressions]

        return Plan(op="CREATE", table=table, schema=None, columns=columns)

    def _parse_insert(self, parsed):
        """Handles INSERT operation"""
        schema_expr = parsed.args["this"]
        table = str(schema_expr.this)
        columns = [col.name for col in schema_expr.expressions]

        values_expr = parsed.args["expression"]
        if not values_expr or not values_expr.expressions:
            raise ValueError("Invalid INSERT statement: missing VALUES clause in the query")
        
        rows = []
        for row_expr in values_expr.expressions:
            row_values = [val.this if hasattr(val, "this") else str(val) for val in row_expr.expressions]
            rows.append(row_values)

        return Plan(op="INSERT", table=table, schema=None, columns=columns, rows=rows)

    def _parse_select(self, parsed):
        """Handles SELECT operation"""
        from_expr = parsed.args.get("from")
        table_expr = from_expr.this if from_expr else None
        table = table_expr.name if table_expr else None

        columns = []
        for expr in parsed.expressions:
            if expr.__class__.__name__ == "Star":
                columns.append("*")
            elif hasattr(expr.this, "name"):
                columns.append(expr.this.name)
            else:
                columns.append("UNKNOWN")
                
        return Plan(op="SELECT", table=table, schema=None, columns=columns)

### use to test the SQLParser ###
if __name__ == "__main__":
    sql_parser = SQLParser()
    test_query = "CREATE TABLE test_table (first_name TEXT, last_name TEXT, age INT) WITH (FORMAT=iceberg)"
    plan = sql_parser.parse_sql(test_query)
    if plan:
        print(plan)
    else:
        print("Failed to parse the SQL query")