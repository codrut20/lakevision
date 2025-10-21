import humanize
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/v1/lakehouse/stats", tags=["Homepage"])
async def get_lakehouse_stats():
    """
    Get high-level statistics about the entire lakehouse.
    """
    from app.dependencies import total_lakehouse_size, namespaces, ns_tables
    try:
        # Calculate total tables from the cached ns_tables dict
        total_tables = sum(len(tables) for tables in ns_tables.values())
    except Exception:
        total_tables = 0 # In case ns_tables isn't populated yet
        
    return {
        "total_namespaces": len(namespaces),
        "total_tables": total_tables,
        "total_data_size_bytes": total_lakehouse_size,
        "total_data_size_human": humanize.naturalsize(total_lakehouse_size)
    }