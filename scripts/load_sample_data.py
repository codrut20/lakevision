import logging
import os
import uuid
import fsspec
import pyarrow.parquet as pq
import pyarrow as pa
from pyiceberg import catalog
from pyiceberg.io import PY_IO_IMPL, FSSPEC_FILE_IO
from urllib.parse import urlparse
import random
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from datetime import date, time, datetime, timezone
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- Read environment ---
uri = os.getenv("PYICEBERG_CATALOG__DEFAULT__URI")
warehouse_uri = os.getenv("PYICEBERG_CATALOG__DEFAULT__WAREHOUSE")

if not uri or not warehouse_uri:
    raise RuntimeError("Missing required environment variables: PYICEBERG_CATALOG__DEFAULT__URI and/or WAREHOUSE")

warehouse_path = urlparse(warehouse_uri).path
sqlite_path = urlparse(uri).path

TABLE_NAME = "default.taxi_dataset"
DATA_URL = "/Users/codrut/Downloads/yellow_tripdata_2023-01.parquet"

def ensure_warehouse():
    os.makedirs(warehouse_path, exist_ok=True)
    logger.info(f"✅ Warehouse directory ensured at: {warehouse_path}")


def load_catalog():
    logger.info("🔗 Initializing Iceberg catalog using SQLite backend...")
    return catalog.load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{sqlite_path}",
            "warehouse": f"file://{warehouse_path}",
            PY_IO_IMPL: FSSPEC_FILE_IO,
        },
    )


def download_data():
    logger.info(f"🌐 Downloading sample data from: {DATA_URL}")
    fs = fsspec.filesystem("https")
    return pq.read_table(DATA_URL)


def create_table_if_missing(cat):
    if cat.table_exists(TABLE_NAME):
        logger.info(f"📄 Table already exists: {TABLE_NAME}")
    else:

        logger.info(f"🛠️ Creating new table: {TABLE_NAME}")
        table = download_data()
        cat.create_table(identifier=TABLE_NAME, schema=table.schema)
        cat.load_table(TABLE_NAME).append(table)
        logger.info(f"✅ Loaded {table.num_rows} rows into {TABLE_NAME}")
    
    if cat.table_exists("default.small_table"):
        logger.info(f"📄 Table already exists: default.small_table")
    else:
        data = {
        'col_1': [11, 12, 8, 9],
        'col_2': [False, True, True, False],
        }

        table_data = pa.Table.from_pydict(data)
        cat.create_table(identifier="default.small_table", schema=table_data.schema)
        cat.load_table("default.small_table").append(table_data)
        logger.info(f"✅ Loaded {table_data.num_rows} rows into default.small_table")
    cat.drop_table("default.data_types")
    if cat.table_exists("default.data_types"):
        logger.info(f"📄 Table already exists: default.data_types")
    else:
        all_types_schema = Schema(
            NestedField(field_id=1, name="bool_col", field_type=BooleanType(), required=False),
            NestedField(field_id=2, name="int_col", field_type=IntegerType(), required=False),
            NestedField(field_id=3, name="long_col", field_type=LongType(), required=False),
            NestedField(field_id=4, name="float_col", field_type=FloatType(), required=False),
            NestedField(field_id=5, name="double_col", field_type=DoubleType(), required=False),
            NestedField(field_id=6, name="date_col", field_type=DateType(), required=False),
            NestedField(field_id=7, name="time_col", field_type=TimeType(), required=False),
            NestedField(field_id=8, name="timestamp_col", field_type=TimestampType(), required=False),
            NestedField(field_id=9, name="timestamptz_col", field_type=TimestamptzType(), required=False),
            NestedField(field_id=10, name="string_col", field_type=StringType(), required=False),
            NestedField(field_id=11, name="uuid_col", field_type=UUIDType(), required=False),
            NestedField(field_id=12, name="fixed_col", field_type=FixedType(length=5), required=False),
            NestedField(field_id=13, name="binary_col", field_type=BinaryType(), required=False),
            NestedField(
                field_id=14, name="list_col",
                field_type=ListType(element_id=15, element_type=StringType(), element_required=False),
                required=False
            ),
            NestedField(
                field_id=16, name="map_col",
                field_type=MapType(key_id=17, key_type=StringType(), value_id=18, value_type=IntegerType(), value_required=False),
                required=False
            ),
            NestedField(
                field_id=19, name="struct_col",
                field_type=StructType(
                    NestedField(field_id=20, name="sub_field1", field_type=StringType(), required=False),
                    NestedField(field_id=21, name="sub_field2", field_type=LongType(), required=False),
                ),
                required=False
            ),
        )

        table = cat.create_table(identifier="default.data_types", schema=all_types_schema)

        data = {
            "bool_col": [random.choice([True, False, None]) for _ in range(10)],
            "int_col": [random.randint(0, 100) if random.random() > 0.1 else None for _ in range(10)],
            "long_col": [random.randint(100000, 900000) if random.random() > 0.1 else None for _ in range(10)],
            "float_col": [round(random.uniform(0, 100), 2) if random.random() > 0.1 else None for _ in range(10)],
            "double_col": [round(random.uniform(1000, 2000), 5) if random.random() > 0.1 else None for _ in range(10)],
            "date_col": [date(2024, random.randint(1, 12), random.randint(1, 28)) if random.random() > 0.1 else None for _ in range(10)],
            "time_col": [time(random.randint(0, 23), random.randint(0, 59), second=random.randint(0, 59), microsecond=random.randint(0, 999999)) if random.random() > 0.1 else None for _ in range(10)],
            "timestamp_col": [datetime(2024, random.randint(1, 12), random.randint(1, 28), random.randint(0, 23), random.randint(0, 59)) if random.random() > 0.1 else None for _ in range(10)],
            "timestamptz_col": [datetime(2024, random.randint(1, 12), random.randint(1, 28), random.randint(0, 23), random.randint(0, 59), tzinfo=timezone.utc) if random.random() > 0.1 else None for _ in range(10)],
            "string_col": [f"string_row_{i}" if random.random() > 0.1 else None for i in range(10)],
            "uuid_col": [uuid.uuid4().bytes if random.random() > 0.1 else None for _ in range(10)],
            "fixed_col": [os.urandom(5) if random.random() > 0.1 else None for _ in range(10)],
            "binary_col": [os.urandom(random.randint(8, 16)) if random.random() > 0.1 else None for _ in range(10)],
            "list_col": [[f"item_{i}", f"item_{j}"] if random.random() > 0.2 else None for i, j in zip(range(10), range(10, 20))],
            "map_col": [{f"key_{i}": i} if random.random() > 0.2 else None for i in range(10)],
            "struct_col": [{"sub_field1": f"struct_str_{i}", "sub_field2": i * 1000} if random.random() > 0.2 else None for i in range(10)],
        }

        arrow_table = pa.Table.from_pydict(data, schema=table.schema().as_arrow())
        table.append(arrow_table)


def main():
    ensure_warehouse()
    cat = load_catalog()
    cat.create_namespace_if_not_exists("default")
    create_table_if_missing(cat)


if __name__ == "__main__":
    main()
