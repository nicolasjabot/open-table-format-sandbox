from dataclasses import dataclass
from typing import List, Optional
from enum import Enum

class SQLOperations(Enum):
    """This class is an Enum representing the supported SQL commands.
    We will be using it in different components of the engine, parser etc to 
    cleanly replace if/elif logic with direct function routing.
    Can be extended later with DROP, ALTER etc operations"""
    SELECT = "SELECT"
    INSERT = "INSERT"
    CREATE = "CREATE"

    def __str__(self):
        return self.value

@dataclass
class Plan:
    """A Normalized SQL operation.
    Typed plans will be easy to use into engine/parser/catalog/storage components"""
    op: str  # Operation (ex: CREATE, INSERT, SELECT)
    table: str 
    schema: Optional[str]
    columns: List[tuple]  # containing column name, and type
    rows: Optional[List[List]] = None  # going to be used for INSERT only