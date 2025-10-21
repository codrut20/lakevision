import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import JSONResponse
from threading import Timer
from app import config
from app.api_utils import CleanJSONResponse
from app.exceptions import LVException
from app.dependencies import (
    background_job_storage, schedule_storage,
    clean_cache, refresh_namespace_and_tables,
    refresh_total_lake_size
)
from app.routers import auth, tables, insights, jobs, homepage

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Application Lifespan (Startup/Shutdown Events) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application startup...")
    # Connect to databases and create tables
    background_job_storage.connect()
    background_job_storage.ensure_table()
    schedule_storage.connect()
    schedule_storage.ensure_table()
    
    # Start periodic maintenance tasks
    clean_cache()

    # These are slow. Run them in a separate thread after a 1-sec delay
    # so they don't block startup.
    Timer(1.0, refresh_namespace_and_tables).start()
    
    # Stagger this one slightly. It has a built-in check
    # to wait for namespaces/tables to be loaded.
    Timer(2.0, refresh_total_lake_size).start()

    print("Startup complete.")
    yield
    print("Application shutdown...")
    background_job_storage.disconnect()
    schedule_storage.disconnect()
    print("Shutdown complete.")

# --- FastAPI App Initialization ---
app = FastAPI(
    default_response_class=CleanJSONResponse,
    lifespan=lifespan
)

# --- Middleware ---
app.add_middleware(SessionMiddleware, secret_key=config.SECRET_KEY, max_age=7200)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Exception Handlers ---
@app.exception_handler(LVException)
async def lv_exception_handler(request: Request, exc: LVException):
    return JSONResponse(
        status_code=418,  # Using 418 as in the original code
        content={"name": exc.name, "message": exc.message},
    )

# --- Include Routers ---
app.include_router(auth.router)
app.include_router(tables.router)
app.include_router(insights.router)
app.include_router(jobs.router)
app.include_router(homepage.router)