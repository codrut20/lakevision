from fastapi import FastAPI, Depends, HTTPException, Request, Response, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import JSONResponse
from app.lakeviewer import LakeView
from app.insights.runner import InsightsRunner
from fastapi import Query
from typing import Generator, Any, List, Optional, Literal, Dict
from pyiceberg.table import Table
import time, os, requests
from threading import Timer
from pydantic import BaseModel, Field
import importlib
import logging
import json
import numpy as np
import math
import decimal
import datetime as dt
import uuid
import pandas as pd
from app.insights.rules import RuleOut, ALL_RULES_OBJECT
from app.storage import get_storage
from datetime import datetime, timezone
from croniter import croniter
from app.scheduler import job_storage as schedule_storage, JobSchedule

AUTH_ENABLED        = True if os.getenv("PUBLIC_AUTH_ENABLED", '')=='true' else False
CLIENT_ID           = os.getenv("PUBLIC_OPENID_CLIENT_ID", '')
OPENID_PROVIDER_URL = os.getenv("PUBLIC_OPENID_PROVIDER_URL", '')
REDIRECT_URI        = os.getenv("PUBLIC_REDIRECT_URI", '')
CLIENT_SECRET       = os.getenv("OPEN_ID_CLIENT_SECRET", '')
TOKEN_URL           = OPENID_PROVIDER_URL+"/token"
SECRET_KEY          = os.getenv("SECRET_KEY", "@#dsfdds1112")

AUTHZ_MODULE = os.getenv("AUTHZ_MODULE_NAME") or "authz"
AUTHZ_CLASS  = os.getenv("AUTHZ_CLASS_NAME") or "Authz"

def _clean_data_recursively(x: Any) -> Any:
    """
    Recursively traverses a data structure to replace special types and values
    that are not JSON serializable.
    """
    if isinstance(x, bytes):
            return '__binary_data__'
    if isinstance(x, (list, tuple, set)):
        return [_clean_data_recursively(v) for v in x]
    if isinstance(x, dict):
        return {k: _clean_data_recursively(v) for k, v in x.items()}

    # Handle numpy types
    if isinstance(x, np.integer):
        return int(x)
    if isinstance(x, np.floating):
        return None if (np.isnan(x) or np.isinf(x)) else float(x)
    if isinstance(x, np.bool_):
        return bool(x)
    if isinstance(x, np.ndarray):
        return x.tolist()

    # Handle standard float
    if isinstance(x, float):
        return None if (math.isnan(x) or math.isinf(x)) else x

    # Handle other common types
    if isinstance(x, (decimal.Decimal)):
        return float(x)
    if isinstance(x, (dt.datetime, dt.date, pd.Timestamp)):
        return x.isoformat()
    if isinstance(x, uuid.UUID):
        return str(x)

    return x

def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """
    Converts a DataFrame to a list of records, then cleans the data for JSON serialization.
    """
    # Convert to dicts first. This preserves nested structures like arrays in cells.
    records = df.to_dict(orient="records")

    # Recursively clean the entire structure. This is the robust way to handle
    # all nested types and values like NaN, Inf, np.int64, etc.
    return _clean_data_recursively(records)

class CleanJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        """
        Renders content to JSON. It assumes content is a standard Python collection
        and runs a final cleaning pass to ensure all nested values are serializable.
        """
        # The content should no longer be a DataFrame here. We run the recursive
        # cleaner as a robust final step for any content returned by endpoints.
        payload = _clean_data_recursively(content)
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")


# --- Simplified FastAPI Application ---

app = FastAPI(default_response_class=CleanJSONResponse)
# Auto-prefix with current package if user passes a bare name like "authz"
if "." not in AUTHZ_MODULE:
    # assummes the authz module is under app package/folder
    AUTHZ_MODULE = f"app.{AUTHZ_MODULE}"

class LVException(Exception):
    def __init__(self, name: str, message: str):
        self.name = name
        self.message = message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

lv = LakeView()

authz_module = importlib.import_module(AUTHZ_MODULE)
authz_class = getattr(authz_module, AUTHZ_CLASS)
authz_ = authz_class()

app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, max_age=7200) # 7200 = 2 hrs
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = Config(environ={
    'AUTHLIB_INSECURE_TRANSPORT': '1',  # Only for local development; remove in production
})
oauth = OAuth(config)

oauth.register(
    name="openid",
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    server_metadata_url=f"{OPENID_PROVIDER_URL}/.well-known/openid-configuration",
    client_kwargs={
        "scope": "email"
    }
)

page_session_cache = {}
CACHE_EXPIRATION = 4 * 60 # 4 minutes
namespaces = None
ns_tables = None

@app.exception_handler(LVException)
async def lv_exception_handler(request: Request, exc: LVException):
    return JSONResponse(
        status_code=418,
        content={"message": exc.message, "name": exc.name},
    )

def load_table(table_id: str) -> Table: #Generator[Table, None, None]:    
    try:
        logging.info(f"Loading table {table_id}")
        table = lv.load_table(table_id)
        return table  # This makes it a generator
    except Exception as e:
        raise HTTPException(status_code=404, detail="Table not found")
    finally:
        # Optional cleanup
        logging.info(f"Finished with loading table {table_id}")

# Dependency to load the table only once
def get_table(request: Request, table_id: str):
    if AUTH_ENABLED:
        user = check_auth(request)
        if not user:
            raise HTTPException(status_code=401, detail="User Not logged in")
    page_session_id = request.headers.get("X-Page-Session-ID")    
    if not page_session_id:
        raise HTTPException(status_code=400, detail="Missing X-Page-Session-ID header")
    cache_key = f"{page_session_id}_{table_id}"

    if cache_key not in page_session_cache:
        tbl = load_table(table_id)
        page_session_cache[cache_key] = (tbl, time.time())
    else:
        tbl, timestamp = page_session_cache[cache_key]
    return tbl    

def check_auth(request: Request):
    user = request.session.get("user")
    if user:
        return user
    return None

@app.get("/api/namespaces")
def read_namespaces(refresh=False):    
    ret = []
    global namespaces
    if refresh or len(namespaces)==0:
        namespaces = lv.get_namespaces()
    for idx, ns in enumerate(namespaces):
        ret.append({"id": idx, "text": ".".join(ns)})        
    return ret

@app.get("/api/namespaces/{namespace}/special-properties")
def read_namespaces_special_properties(namespace: str):
    return authz_.get_namespace_special_properties(namespace)

@app.get("/api/tables/{table_id}/special-properties")
def read_table_special_properties(table_id: str):
    return authz_.get_table_special_properties(table_id)

@app.get("/api/tables/{table_id}/snapshots")
def read_table_snapshots(table: Table = Depends(get_table)):
    return _df_to_records(lv.get_snapshot_data(table))

@app.get("/api/tables/{table_id}/partitions", status_code=status.HTTP_200_OK)
def read_table_partitions(request: Request, response: Response, table_id: str, table: Table = Depends(get_table)):
    if not authz_.has_access(request, response, table_id):        
        return
    return _df_to_records(lv.get_partition_data(table))


@app.get("/api/tables/{table_id}/sample", status_code=status.HTTP_200_OK)    
def read_sample_data(request: Request, response: Response, table_id: str, sql = None, table: Table = Depends(get_table), sample_limit: int=100):
    if not authz_.has_access(request, response, table_id):        
        return
    try:    
        res =  lv.get_sample_data(table, sql, sample_limit)
        return _df_to_records(res)
    except Exception as e:
        logging.error(str(e))
        raise LVException("err", str(e))

@app.get("/api/tables/{table_id}/schema")    
def read_schema_data(table: Table = Depends(get_table)):
    return _df_to_records(lv.get_schema(table))

@app.get("/api/tables/{table_id}/summary")    
def read_summary_data(table: Table = Depends(get_table)):
    return lv.get_summary(table)

@app.get("/api/tables/{table_id}/properties")    
def read_properties_data(table: Table = Depends(get_table)):
    return lv.get_properties(table)

@app.get("/api/tables/{table_id}/partition-specs")    
def read_partition_specs(table: Table = Depends(get_table)):
    return lv.get_partition_specs(table)

@app.get("/api/tables/{table_id}/sort-order")    
def read_sort_order(table: Table = Depends(get_table)):
    return lv.get_sort_order(table)

@app.get("/api/tables/{table_id}/data-change")    
def read_data_change(table: Table = Depends(get_table)):
    return _df_to_records(lv.get_data_change(table))

@app.get("/")
def root(request: Request):
    if AUTH_ENABLED:
        user = check_auth(request)
        if not user:
            return {"message": "You are not logged in."}
        else:
            return {"message": f"You are logged in as {user}"}
    return "Hello, no auth enabled"
       
@app.get("/api/tables")
def read_tables(namespace: str = None, refresh=False, user=Depends(check_auth)):    
    if AUTH_ENABLED and not user:
        raise HTTPException(status_code=401, detail="User Not logged in")
    ret = []
    if not namespace:
        global namespaces
        global ns_tables        
        if refresh:            
            namespaces = lv.get_namespaces()
            ns_tables = lv.get_all_table_names(namespaces)
        for namespace, tables in ns_tables.items():           
            for idx, table in enumerate(tables):            
                ret.append({"id": idx, "text": table, "namespace": ".".join(namespace)})
        return ret
    for idx, table in enumerate(lv.get_tables(namespace)):
        ret.append({"id": idx, "text": table[-1], "namespace": namespace})        
    return ret

@app.get("/api/login")
async def login(request: Request):
    redirect_uri = REDIRECT_URI
    return await oauth.openid.authorize_redirect(request, redirect_uri)

class TokenRequest(BaseModel):
    code: str

@app.post("/api/auth/token")
def get_token(request: Request, tokenReq: TokenRequest):
    data = {
        "grant_type": "authorization_code",
        "code": tokenReq.code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
    }    
    response = requests.post(TOKEN_URL, data=data)    
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to exchange code for token")
    r2 = requests.get(f"{OPENID_PROVIDER_URL}/userinfo?access_token={response.json()['access_token']}")
    user_email = r2.json()['email'] 
    request.session['user'] = user_email
    return user_email

@app.get("/api/logout")
def logout(request: Request):
    request.session.pop("user", None)
    return RedirectResponse(REDIRECT_URI)

# Insights API
@app.get("/api/tables/{table_id}/insights/latest")
def get_table_insights(
    table_id: str,
    size: int = Query(5, ge=1, description="The number of items per page.")
):
    """
    Retrieves a paginated list of the latest insight runs for a specific table.
    """
    runner = InsightsRunner(lv)

    # Call the updated method that returns paginated data
    paginated_data = runner.get_latest_run(table_id, size=size)

    # Convert the items for the current page to dictionaries
    items_as_dicts = [run.__dict__ for run in paginated_data]

    # Return the final response in the format expected by the frontend
    return items_as_dicts


# Insights API
@app.get("/api/tables/{table_id}/insights")
def get_table_insights(table_id: str, rule_ids: Optional[List[str]] = Query(
        None, 
        alias="rule_ids", 
        description="List of rule IDs to run. If omitted, all rules will be run."
    )):
    runner = InsightsRunner(lv)
    try:
        results = runner.run_for_table(table_id, rule_ids)
        return [insight.__dict__ for insight in results]
    except Exception as e:
        logging.error(f"Insights error for table {table_id}: {str(e)}")
        raise LVException("insight", f"Failed to compute insights for table {table_id}: {e}")


@app.get("/api/namespaces/{namespace}/insights")
def get_namespace_insights(
    namespace: str, 
    recursive: bool = Query(True, description="Search nested namespaces recursively?")
):
    runner = InsightsRunner(lv)
    try:
        results = runner.run_for_namespace(namespace, recursive=recursive)
        # results is a dict of {qualified_table_name: [Insight, ...]}
        return {k: [ins.__dict__ for ins in v] for k, v in results.items()}
    except Exception as e:
        logging.error(f"Insights error for namespace {namespace}: {str(e)}")
        raise LVException("insight", f"Failed to compute insights for namespace {namespace}: {e}")


@app.get("/api/lakehouse/insights/rules", response_model=List[RuleOut])
def get_lakehouse_insights():
    return ALL_RULES_OBJECT


@app.get("/api/lakehouse/insights")
def get_lakehouse_insights():
    runner = InsightsRunner(lv)
    try:
        results = runner.run_for_lakehouse()
        return {k: [ins.__dict__ for ins in v] for k, v in results.items()}
    except Exception as e:
        logging.error(f"Insights error for lakehouse: {str(e)}")
        raise LVException("insight", f"Failed to compute lakehouse insights: {e}")


def clean_cache():
    """Remove expired entries from the cache."""
    current_time = time.time()
    keys_to_delete = [
        key for key, (_, timestamp) in page_session_cache.items()
        if current_time - timestamp > CACHE_EXPIRATION
    ]
    for key in keys_to_delete:
        del page_session_cache[key]

# Schedule periodic cleanup every minute
def schedule_cleanup():
    clean_cache()
    Timer(60, schedule_cleanup).start()

# refresh namespaces and tables every 65 mins
def refresh_namespace_and_tables():
    read_tables(refresh=True, user='maintenance')    
    Timer(3900, refresh_namespace_and_tables).start()

schedule_cleanup()
refresh_namespace_and_tables()

# --- In-Memory Storage (for demonstration purposes) ---
# In a real application, you would use a database (like Redis or your SQL DB)
# to store the status and results of jobs so they persist across server restarts.
job_storage: Dict[str, Dict[str, Any]] = {}


# --- Pydantic Models for API Data Structure ---

class RunRequest(BaseModel):
    namespace: str
    table_name: str | None = None
    rules_requested: List[str]

class RunResponse(BaseModel):
    run_id: str
    message: str = "Job accepted and is running in the background."

class StatusResponse(BaseModel):
    run_id: str
    status: Literal["pending", "running", "complete", "failed"]
    details: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    results: List[Dict[str, Any]] | None = None


# --- The Long-Running Task Logic ---

def run_insights_job(run_id: str, namespace: str, table_name: str | None, rules: List[str]):
    """
    This is the core function that runs in the background.
    It simulates a long-running process to generate insights.
    """
    print(f"[{datetime.now()}] Starting background job: {run_id}")
    
    job_storage[run_id].update({
        "status": "running",
        "started_at": datetime.now(),
        "details": f"Processing {len(rules)} rules for '{namespace}'..."
    })

    try:

        runner = InsightsRunner(lv)
        results = runner.run_for_table(table_identifier=namespace+"."+table_name, rule_ids=rules)

        print(f"[{datetime.now()}] Job {run_id} completed successfully.")
        job_storage[run_id].update({
            "status": "complete",
            "finished_at": datetime.now(),
            "details": "Job finished successfully.",
            "results": results,
        })

    except Exception as e:
        print(f"[{datetime.now()}] Job {run_id} failed: {e}")
        job_storage[run_id].update({
            "status": "failed",
            "finished_at": datetime.now(),
            "details": f"An error occurred: {str(e)}",
        })


@app.post("/api/start-run", response_model=RunResponse, status_code=202)
def start_manual_run(
    run_request: RunRequest,
    background_tasks: BackgroundTasks
):
    """
    Accepts a request to run an insight job, starts it in the background,
    and immediately returns a run_id for status tracking.
    """
    run_id = str(uuid.uuid4())
    
    # Store initial "pending" state
    job_storage[run_id] = {"status": "pending"}
    
    # Add the long-running function to the background tasks
    background_tasks.add_task(
        run_insights_job,
        run_id,
        run_request.namespace,
        run_request.table_name,
        run_request.rules_requested,
    )
    
    return {"run_id": run_id}


@app.get("/run-status/{run_id}", response_model=StatusResponse)
def get_run_status(run_id: str):
    """
    Retrieves the current status and results of a background job.
    """
    job = job_storage.get(run_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    
    return StatusResponse(run_id=run_id, **job)

class JobScheduleRequest(BaseModel):
    """Defines the user-provided data for creating a new scheduled job."""
    namespace: str
    table_name: Optional[str] = None
    rules_requested: List[str]
    cron_schedule: str # e.g., "0 * * * *" for hourly
    created_by: str # e.g., user's email or ID

class JobScheduleResponse(JobScheduleRequest):
    """Defines the full job schedule object as stored in the DB."""
    id: str
    is_enabled: bool = True
    next_run_timestamp: datetime
    last_run_timestamp: Optional[datetime] = None
    created_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

@app.post("/api/schedules", response_model=JobScheduleResponse, status_code=201)
def create_schedule(schedule_request: JobScheduleRequest):
    """
    Creates and stores a new job schedule.
    The cron_schedule format is based on standard cron syntax.
    """
    try:
        # Validate the cron expression and calculate the first run time
        now = datetime.now(timezone.utc)
        iterator = croniter(schedule_request.cron_schedule, now)
        next_run = iterator.get_next(datetime)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid cron_schedule format: '{schedule_request.cron_schedule}'. Error: {e}"
        )
    
    # Create an instance of the JobSchedule dataclass for internal use/storage
    new_schedule = JobSchedule(
        next_run_timestamp=next_run,
        **schedule_request.model_dump()
    )
    
    # Store the dataclass object in our "database" after converting it to a dict
    schedule_storage.save(new_schedule)
    
    print(f"Created new schedule {new_schedule.id}. Next run at: {next_run}")
    
    # Return the new schedule object; FastAPI will convert it to the response model
    return new_schedule