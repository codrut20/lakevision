import os
from dataclasses import dataclass, asdict, field

from app.storage import get_storage
from app.storage.sqlalchemy_adapter import SQLAlchemyStorage

@dataclass
class User:
    id: str
    name: str
    email: str
    is_active: bool = True
    test: dict = field(default_factory=dict)

def run_demo():
    """Demonstrates CRUD operations with the storage."""
    
    # --- SCENARIO 1: Use SQLAlchemy with SQLite in-memory ---
    print("\n--- SCENARIO 1: SQLAlchemy with SQLite ---")
    os.environ['DATABASE_URL'] = "sqlite:///:memory:"
    storage_sqlalchemy = get_storage(model=User)
    run_crud_for_storage(storage_sqlalchemy)

    # --- SCENARIO 4: Use SQLAlchemy with Postgres ---
    print("\n--- SCENARIO 4: SQLAlchemy with Postgres ---")
    os.environ['DATABASE_URL'] = "postgresql://db_user:testpass@192.168.1.101:5432/lakevision"
    storage_sqlalchemy2 = get_storage(model=User)
    run_crud_for_storage(storage_sqlalchemy2)


def run_crud_for_storage(storage: SQLAlchemyStorage[User]):
    """Generic function to run CRUD tests on a given storage adapter."""
    try:
        print(f"--- Testing with {storage.__class__.__name__} for model '{User.__name__}' ---")
        storage.connect()
        storage.ensure_table()

        # 1. Save users
        print("\n1. Saving users...")
        storage.save(User(id="user:1", name="Alice", email="alice@example.com", test={"test":"test"}))
        storage.save(User(id="user:2", name="Bob", email="bob@example.com", is_active=False))
        storage.save(User(id="user:3", name="Alice", email="alice_2@example.com"))

        # 2. Test get_by_attribute
        print("\n2. Getting users by attribute (name='Alice')...")
        alices = storage.get_by_attribute(attribute="name", value="Alice")
        print(f"Found {len(alices)} users with the name Alice: {alices}")
        assert len(alices) == 2
        
        # 3. Test get_by_id
        bob = storage.get_by_id("user:2")
        print(f"\n3. Retrieved user by ID: {bob}")
        assert bob.name == "Bob"

        # 4. Test delete
        print("\n4. Deleting Bob...")
        storage.delete("user:2")
        all_users = storage.get_all()
        print(f"Get All after delete: Found {len(all_users)} user(s).")
        assert len(all_users) == 2

    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if storage:
            storage.disconnect()

if __name__ == "__main__":
    # To run this:
    # 1. pip install sqlalchemy duckdb
    # 2. Run the script.
    run_demo()