import dataclasses
import json
from typing import Any, Dict, List, Optional, Type
from sqlalchemy import (create_engine, text, inspect, Table, Column, MetaData,
                          String, Integer, Float, Boolean, Text, bindparam)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from typing import get_origin, Literal, Any, List, get_args, Union
from .interface import StorageInterface, T, AggregateFunction
from datetime import datetime
from dateutil.parser import parse

class SQLAlchemyStorage(StorageInterface[T]):
    """
    Stores a dataclass in a table with columns matching the dataclass fields.
    """
    def __init__(self, db_url: str, model: Type[T]):
        super().__init__(model)
        self._db_url = db_url
        self._engine: Optional[Engine] = None
        self._field_names = {f.name for f in dataclasses.fields(self.model)}
        
        self._complex_fields = set()
        self._datetime_fields = set()
        for f in dataclasses.fields(self.model):
            origin = get_origin(f.type)
            # Handle simple list, dict, tuple
            if origin in [list, dict, tuple]:
                self._complex_fields.add(f.name)
            # Handle Optional[list], etc. which becomes Union[list, None]
            elif origin is Union:
                # Check the types inside the Union
                for arg in get_args(f.type):
                    if get_origin(arg) in [list, dict, tuple]:
                        self._complex_fields.add(f.name)
                        break
                    if arg is datetime:
                        self._datetime_fields.add(f.name)
                        break
            elif f.type is datetime:
                self._datetime_fields.add(f.name)

        print(f"✅ Initialized SQLAlchemyStorage for model '{model.__name__}'")
        print(f"   Complex fields (JSON): {self._complex_fields}")
        print(f"   Datetime fields: {self._datetime_fields}")

    def connect(self) -> None:
        if not self._engine:
            self._engine = create_engine(self._db_url)

    def disconnect(self) -> None:
        if self._engine:
            self._engine.dispose()

    def _get_engine(self) -> Engine:
        if not self._engine:
            raise ConnectionError("Database not connected.")
        return self._engine
    
    def _map_type(self, py_type: Type) -> Any:
        """Maps Python types to SQLAlchemy types."""
        if py_type is int: return Integer
        if py_type is float: return Float
        if py_type is bool: return Boolean
        # Use Text for strings and complex types that will be serialized to JSON
        if py_type in [str, list, dict, tuple]: return Text
        # Fallback for other types
        return Text

    def _serialize_row(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Serializes complex fields in a dictionary to JSON strings."""
        serialized = row_dict.copy()
        for field_name in self._complex_fields:
            if field_name in serialized and serialized[field_name] is not None:
                serialized[field_name] = json.dumps(serialized[field_name])
        return serialized

    def _deserialize_row(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Deserializes fields from JSON strings back into Python objects."""
        deserialized = row_dict.copy()
        
        # Deserialize complex JSON fields
        for field_name in self._complex_fields:
            if field_name in deserialized and isinstance(deserialized[field_name], str):
                try:
                    deserialized[field_name] = json.loads(deserialized[field_name])
                except json.JSONDecodeError:
                    pass
        
        # Now, parse any fields that should be datetime objects
        for field_name in self._datetime_fields:
            if field_name in deserialized and isinstance(deserialized[field_name], str):
                try:
                    deserialized[field_name] = parse(deserialized[field_name])
                except (ValueError, TypeError):
                    deserialized[field_name] = None # Set to None if parsing fails

        return deserialized

    def ensure_table(self) -> None:
        engine = self._get_engine()
        if not inspect(engine).has_table(self.table_name):
            metadata = MetaData()
            columns = []
            for field in dataclasses.fields(self.model):
                is_primary_key = field.name == 'id'
                # Use String(255) only for the primary key for better indexing
                sqlalchemy_type = String(255) if is_primary_key else self._map_type(field.type)
                columns.append(Column(field.name, sqlalchemy_type, primary_key=is_primary_key))
            Table(self.table_name, metadata, *columns)
            metadata.create_all(engine)
            print(f"Table '{self.table_name}' created with schema.")

    def save(self, item: T) -> None:
        """Saves a single item by wrapping it in a list and calling save_many."""
        self.save_many([item])

    def save_many(self, items: List[T]) -> None:
        """
        Atomically saves a list of items using a "delete-then-insert" pattern.

        The entire operation is performed within a single transaction. It first
        deletes all records from the database that have an ID matching any of
        the provided items. Then, it performs a bulk insert of all the items.

        This is efficient for creating or overwriting many records at once.

        Args:
            items: A list of model instances to be saved.
        """
        if not items:
            return  # Do nothing if the list is empty

        engine = self._get_engine()

        # 1. Prepare all data and collect IDs in one go.
        serialized_items = []
        item_ids = []
        for item in items:
            item_dict = dataclasses.asdict(item)
            item_id = item_dict.get('id')
            
            # Add ID to the list for the DELETE step
            if item_id is not None:
                item_ids.append(item_id)
            
            # Prepare the data for the INSERT step
            serialized_items.append(self._serialize_row(item_dict))

        # Ensure we have something to insert
        if not serialized_items:
            return

        # 2. Perform the entire operation in a single transaction
        with engine.begin() as conn:
            # Step A: Delete all existing records matching the provided IDs.
            # Using 'WHERE id IN (...)' is highly efficient for bulk deletes.
            if item_ids:
                # SQLAlchemy automatically handles the expansion of the tuple for the IN clause
                delete_stmt = text(f"DELETE FROM {self.table_name} WHERE id IN :ids")
                conn.execute(delete_stmt, {"ids": tuple(item_ids)})

            # Step B: Perform a bulk insert with all the new item data.
            # We can get the structure from the first item, as they are all the same.
            first_item = serialized_items[0]
            columns = ", ".join(f'"{key}"' for key in first_item.keys()) # Quote column names
            placeholders = ", ".join(f":{key}" for key in first_item.keys())
            
            insert_stmt = text(f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})")
            
            # SQLAlchemy's execute method handles a list of dicts as a bulk "executemany"
            conn.execute(insert_stmt, serialized_items)

    def get_by_id(self, item_id: Any) -> Optional[T]:
        engine = self._get_engine()
        with engine.connect() as conn:
            stmt = text(f"SELECT * FROM {self.table_name} WHERE id = :id")
            result = conn.execute(stmt, {"id": item_id}).mappings().first()
        
        if not result:
            return None
        
        deserialized_result = self._deserialize_row(dict(result))
        return self.model(**deserialized_result)

    def get_all(self) -> List[T]:
        engine = self._get_engine()
        with engine.connect() as conn:
            stmt = text(f"SELECT * FROM {self.table_name}")
            results = conn.execute(stmt).mappings().all()
        
        deserialized_results = [self._deserialize_row(dict(row)) for row in results]
        return [self.model(**row) for row in deserialized_results]

    def get_by_attributes(
        self,
        criteria: dict[str, Any],
        skip: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[T]:
        """
        Retrieves records from the database that match all specified criteria,
        with optional pagination and support for IN clauses.
        """
        # 1. Validate all incoming attributes
        for attribute in criteria.keys():
            if attribute not in self._field_names:
                raise ValueError(f"'{attribute}' is not a valid field in {self.model.__name__}")

        # 2. Build the WHERE clause and parameters dynamically
        params = {}
        if not criteria:
            where_clause = ""
        else:
            clauses = []
            # Build clauses and params together to handle IN lists correctly
            for attr, value in criteria.items():
                if isinstance(value, list):
                    if not value:
                        # If the list is empty, create a condition that is always false
                        clauses.append("1=0")
                        continue

                    # Create unique placeholders like :status_0, :status_1, etc.
                    param_names = [f"{attr}_{i}" for i in range(len(value))]
                    # Create the IN clause string: e.g., "status IN (:status_0, :status_1)"
                    clauses.append(f"{attr} IN ({', '.join(':' + p for p in param_names)})")
                    
                    # Add the individual values to the params dict
                    for p_name, p_value in zip(param_names, value):
                        params[p_name] = p_value
                else:
                    # Handle standard equals (=) clause for non-list values
                    clauses.append(f"{attr} = :{attr}")
                    params[attr] = json.dumps(value) if attr in self._complex_fields else value
            
            where_clause = "WHERE " + " AND ".join(clauses)
                
        # 3. Construct the final SQL statement
        sql_query = f"SELECT * FROM {self.table_name} {where_clause}"

        if 'created_at' in self._field_names:
            sql_query += " ORDER BY created_at DESC"
        elif 'run_timestamp' in self._field_names:
            sql_query += " ORDER BY run_timestamp DESC"

        if limit is not None:
            sql_query += " LIMIT :limit"
            params['limit'] = limit

        if skip is not None:
            sql_query += " OFFSET :skip"
            params['skip'] = skip

        engine = self._get_engine()
        with engine.connect() as conn:
            stmt = text(sql_query)
            
            # 4. Execute with the correctly built parameters
            results = conn.execute(stmt, params).mappings().all()

        deserialized_results = [self._deserialize_row(dict(row)) for row in results]
        return [self.model(**row) for row in deserialized_results]
    
    def get_aggregate(
        self,
        func: AggregateFunction,
        column: str,
        criteria: dict[str, Any] | None = None,
        group_by: List[str] | None = None
    ) -> Any:
        """
        Calculates an aggregate value (MIN, MAX, COUNT, etc.) for a column.

        Args:
            func: The aggregate function to use ('MIN', 'MAX', 'AVG', 'SUM', 'COUNT').
            column: The column to apply the function to. Use '*' for COUNT(*).
            criteria: Optional dictionary to filter rows with a WHERE clause.
            group_by: Optional list of columns to group the results by.

        Returns:
            - A single value if 'group_by' is not used.
            - A list of dictionaries if 'group_by' is used.
        """
        # 1. --- Security and Validation ---
        func = func.upper()  # Normalize to uppercase
        if func not in ["MIN", "MAX", "AVG", "SUM", "COUNT"]:
            raise ValueError(f"Unsupported aggregate function: '{func}'")

        if column != '*' and column not in self._field_names:
            raise ValueError(f"'{column}' is not a valid field in {self.model.__name__}")

        # 2. --- Build Query Components ---
        # SELECT clause
        select_columns = f"{func}({column}) as result"
        if group_by:
            for col in group_by:
                if col not in self._field_names:
                    raise ValueError(f"'{col}' is not a valid group_by field.")
            group_by_str = ", ".join(group_by)
            select_columns = f"{group_by_str}, {select_columns}"

        # WHERE clause and parameters (reusing logic from get_by_attributes)
        params = {}
        where_clause = ""
        if criteria:
            for attr in criteria.keys():
                if attr not in self._field_names:
                    raise ValueError(f"'{attr}' is not a valid criteria field.")
            
            clauses = [f"{attr} = :{attr}" for attr in criteria.keys()]
            where_clause = "WHERE " + " AND ".join(clauses)
            params = {
                attr: json.dumps(value) if attr in self._complex_fields else value
                for attr, value in criteria.items()
            }

        # GROUP BY clause
        group_by_clause = f"GROUP BY {', '.join(group_by)}" if group_by else ""

        # 3. --- Assemble and Execute ---
        engine = self._get_engine()
        with engine.connect() as conn:
            sql_query = f"""
                SELECT {select_columns}
                FROM {self.table_name}
                {where_clause}
                {group_by_clause}
            """
            stmt = text(sql_query)
            results = conn.execute(stmt, params).mappings().all()

        # 4. --- Format and Return Result ---
        if not results:
            return None if not group_by else []

        if not group_by:
            # Return a single value, e.g., 42
            return results[0]["result"]
        else:
            # Return a list of dicts, e.g., [{'city': 'NYC', 'result': 45}, ...]
            return [dict(row) for row in results]
        
    def execute_raw_select_query(self, sql_query: str, params: dict[str, Any] | None = None) -> List[dict[str, Any]]:
        """
        Executes a raw, parameterized SELECT query and returns the results.

        🚨 SECURITY WARNING: ALWAYS use the 'params' argument for any dynamic values
        to prevent SQL injection.

        Args:
            sql_query: The raw SQL SELECT string to execute.
            params: A dictionary of parameters to be safely bound to the query.

        Returns:
            A list of dictionaries, where each dictionary represents a fetched row.
            
        Raises:
            ValueError: If the provided query is not a SELECT statement.
        """
        # 1. --- Validation Check ---
        # We strip leading whitespace and check the first word (case-insensitive).
        if not sql_query.strip().lower().startswith("select"):
            raise ValueError("This method only supports SELECT queries.")

        engine = self._get_engine()
        with engine.connect() as conn:
            stmt = text(sql_query)
            result = conn.execute(stmt, params or {})
            return [dict(row) for row in result.mappings().all()]

    def get_by_attribute(
        self,
        attribute: str,
        value: Any,
        skip: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[T]:
        """Original single-attribute method, now delegates to the new one."""
        return self.get_by_attributes({attribute: value}, skip, limit)

    def delete(self, item_id: Any) -> None:
        engine = self._get_engine()
        with engine.begin() as conn:
            stmt = text(f"DELETE FROM {self.table_name} WHERE id = :id")
            conn.execute(stmt, {"id": item_id})