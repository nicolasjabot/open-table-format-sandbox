from sqlglot import parse_one
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Plan:
    op: str  # Operation (ex: CREATE, INSERT, SELECT)
    table: str 
    schema: Optional[str]
    columns: List[tuple]  # containing column name, and type

class SQLParser:
    def __init__(self, enforce_engine='iceberg'):
        self.enforce_engine = enforce_engine

    def parse_sql(self, query: str) -> Optional[Plan]:
        try:
            # Parse the query
            parsed = parse_one(query)
            op = parsed.key.upper()
            
            # create operator parser
            if op == "CREATE":
                schema_expr = parsed.args["this"]
                table_expr = schema_expr.this 
                table = table_expr.name    
                columns = [(col.this.name, col.args["kind"].this.value) for col in schema_expr.expressions]

                return Plan(op="CREATE", table=table, schema=None, columns=columns)

            # insert operator parser
            elif op == "INSERT":
                schema_expr = parsed.args["this"]
                table_expr = schema_expr.this
                table = table_expr.name
                columns = [col.name for col in schema_expr.expressions]

                return Plan (op="INSERT", table=table, schema=None, columns=columns)

            # select operator parser
            elif op == "SELECT":
                # Extract table name safely
                from_expr = parsed.args.get("from")
                table_expr = from_expr.this if from_expr else None
                table = table_expr.name if table_expr else None

                # Extract columns
                columns = []
                for expr in parsed.expressions:
                    if expr.__class__.__name__ == "Star":
                        columns.append("*")
                    elif hasattr(expr.this, "name"):
                        columns.append(expr.this.name)
                    else:
                        columns.append("UNKNOWN")

                return Plan(op="SELECT", table=table, schema=None, columns=columns)

            else:
                raise ValueError(f"Unsupported operation: {op}")

        except Exception as e:
            print(f"Error parsing SQL: {e}")
            return None
        

### use to test the SQLParser ###

# def run_sql_parser(query: str):
#     """
#     Function to run the SQL parser and print the resulting Plan.
#     """
#     parser = SQLParser()
#     plan = parser.parse_sql(query)
#     if plan:
#         print(f"Parsed Plan: {plan}")
#     else:
#         print("Failed to parse the SQL query.")

# # Example usage
# if __name__ == "__main__":
#     sample_query = "INSERT INTO test_table (first_name, last_name, age) VALUES ('nico', 'jabot', 27)"
#     run_sql_parser(sample_query)