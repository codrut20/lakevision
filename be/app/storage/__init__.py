import os
from typing import Optional, Type

from .interface import StorageInterface, T
from .sqlalchemy_adapter import SQLAlchemyStorage
from insights.rules import InsightRun

def get_storage(
    model: Type[T],
    db_url: Optional[str] = None
) -> StorageInterface[T]:
    """
    Factory to select a model-aware storage backend.
    """
    db_url = db_url or os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("Database URL is not provided or set in DATABASE_URL.")

    return SQLAlchemyStorage(db_url, model)

#os.environ['DATABASE_URL'] = "sqlite:////tmp/test5.db"
os.environ['DATABASE_URL'] = "postgresql://db_user:testpass@192.168.1.101:5432/lakevision"
run_storage = get_storage(model=InsightRun)
run_storage.connect()
run_storage.ensure_table()
run_storage.disconnect()