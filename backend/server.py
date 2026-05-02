"""
server.py
---------
FastAPI web server wrapping the insider pipeline.

Endpoints:
  GET  /          → health check
  POST /run       → trigger the pipeline now (requires secret header)
  GET  /status    → last run result (in-memory, resets on restart)

Deploy to Railway / Render:
  Start command:  uvicorn server:app --host 0.0.0.0 --port $PORT
  Cron trigger:   POST /run  every weekday at 7:00 AM EST

Environment variables needed (same as .env):
  AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_INSIDER,
  AIRTABLE_TABLE_RUNS, ZAPIER_WEBHOOK_URL, POLYGON_API_KEY,
  MIN_SCORE_FOR_ALERT, RUN_SECRET (new — protects the /run endpoint)
"""

import os
import sys
import logging
import asyncio
from dotenv import load_dotenv
import sqlite3
from contextlib import asynccontextmanager
from collections import deque
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from starlette.concurrency import run_in_threadpool
from pathlib import Path

# Ensure the backend directory is in sys.path so 'main' and 'backtester' can be imported easily
backend_dir = Path(__file__).parent.resolve()
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from market_data import get_spy_gap
from typing import Dict

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Live Log Interceptor ---
log_queue = deque(maxlen=200)
is_pipeline_running = False
is_health_checking = False

class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.append(msg)
        except Exception:
            pass

queue_handler = QueueHandler()
queue_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
# Attach globally to capture all module logs
logging.getLogger().addHandler(queue_handler)

# --- Run History DB ---
HISTORY_DB = backend_dir / "run_history.db"

def get_db_connection():
    conn = sqlite3.connect(str(HISTORY_DB), timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_history_db():
    try:
        with get_db_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    status TEXT NOT NULL,
                    signals INTEGER NOT NULL,
                    message TEXT,
                    duration_sec INTEGER NOT NULL,
                    is_auto INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to initialize history DB: {e}")

def load_history() -> list:
    try:
        with get_db_connection() as conn:
            rows = conn.execute(
                "SELECT time, status, signals, message, duration_sec, is_auto FROM pipeline_runs ORDER BY time DESC"
            ).fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to load history from DB: {e}")
        return []

def save_history_record(record: dict):
    try:
        with get_db_connection() as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (time, status, signals, message, duration_sec, is_auto) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    record["time"],
                    record["status"],
                    record["signals"],
                    record.get("message"),
                    record["duration_sec"],
                    1 if record.get("is_auto") else 0,
                ),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save history record to DB: {e}")

RUN_SECRET = os.getenv("RUN_SECRET", "")  # set this in production!

# In-memory session state
_last_run: dict = {"status": "never", "time": None, "signals": 0, "message": ""}
_last_health_check_results: list = []
_cached_spy_gap: float = 0.0

scheduler = AsyncIOScheduler()

async def execute_pipeline_core(is_auto=False):
    global is_pipeline_running
    if is_pipeline_running: return
    is_pipeline_running = True
    log_queue.clear()
    
    start_time = datetime.now(timezone.utc)
    tag = "(auto)" if is_auto else ""
    logger.info(f"Pipeline started {tag}")
    
    try:
        from main import run
        signals = await run_in_threadpool(run) or []
        status_key = f"completed {tag}".strip()
        msg = f"{len(signals)} signals generated"
    except Exception as e:
        signals = []
        status_key = f"error {tag}".strip()
        msg = str(e)
        logger.exception("Pipeline failed")
    finally:
        is_pipeline_running = False
        duration = (datetime.now(timezone.utc) - start_time).seconds
        
        _last_run["status"] = status_key
        _last_run["time"] = datetime.now(timezone.utc).isoformat()
        _last_run["signals"] = len(signals)
        _last_run["message"] = msg
        
        save_history_record({
            "time": _last_run["time"],
            "status": status_key,
            "signals": len(signals),
            "message": msg,
            "duration_sec": duration,
            "is_auto": is_auto,
        })

async def execute_health_check_core(lookback_days: int = 30):
    global is_health_checking, _last_health_check_results
    if is_health_checking: return
    is_health_checking = True
    log_queue.clear()
    
    logger.info(f"Strategic Health Check started (Lookback: {lookback_days} days)")
    
    end_date   = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)
    start_str  = start_date.strftime("%Y-%m-%d")
    end_str    = end_date.strftime("%Y-%m-%d")
    
    modules = ["Insider", "Technical_Under_5", "Technical_Under_10", "Technical_Under_20"]
    results = []

    try:
        from backtester import run_backtest
        for module in modules:
            logger.info(f"Health check running for: {module}")
            # Offload blocking work to a threadpool
            metrics = await run_in_threadpool(run_backtest, module, start_str, end_str)
            
            if metrics:
                results.append({
                    "module": module,
                    "win_rate": metrics["win_rate"],
                    "avg_return": metrics["average_return"],
                    "total_return": metrics["total_return"],
                    "trades": metrics["total_trades"]
                })
        _last_health_check_results = results
        logger.info(f"Strategic Health Check complete. Analysed {len(results)} modules.")
    except Exception as e:
        logger.exception("Health check failed")
    finally:
        is_health_checking = False

async def automated_pipeline_run():
    logger.info("Triggering automated pipeline run via APScheduler...")
    await execute_pipeline_core(is_auto=True)
    logger.info("Automated pipeline complete. Triggering follow-up 7-day health check...")
    await execute_health_check_core(lookback_days=7)

async def refresh_spy_gap():
    global _cached_spy_gap
    try:
        logger.info("Lifespan: Refreshing cached SPY gap...")
        # Use a timeout of 10s for the threadpool task if possible, 
        # but run_in_threadpool doesn't support timeout directly.
        # get_spy_gap itself should handle its own timeouts.
        _cached_spy_gap = await run_in_threadpool(get_spy_gap)
        logger.info(f"Lifespan: Cached SPY gap updated: {_cached_spy_gap:+.2f}%")
    except Exception as e:
        logger.error(f"Lifespan: Failed to refresh SPY gap: {e}")

async def automated_backtest_run():
    try:
        logger.info("Lifespan: Starting automated backtest run...")
        await refresh_spy_gap()
        await execute_health_check_core(lookback_days=90)
        logger.info("Lifespan: Automated backtest run complete.")
    except Exception as e:
        logger.error(f"Lifespan: Automated backtest run failed: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("INIT: Insider Scanner server starting up (lifespan start)")
    logger.info(f"INIT: System timezone: {os.getenv('TZ', 'Not configured')}")
    
    try:
        init_history_db()
        # Schedule the job to run every Monday-Friday at 7:00 AM EST (New York)
        # This automatically runs the daily scraper pipeline, then triggers a
        # follow-up 7-day health check once the pipeline completes.
        scheduler.add_job(
            automated_pipeline_run, 
            CronTrigger(day_of_week='mon-fri', hour=7, minute=0, timezone='America/New_York'),
            id='daily_pipeline',
            replace_existing=True
        )
        logger.info("INIT: Scheduled daily pipeline for Mon-Fri 7:00 AM EST")
        
        # Schedule strategy health-check backtests every 3 days as a longer-term
        # analytics task in addition to the daily 7-day follow-up run.
        scheduler.add_job(
            automated_backtest_run,
            IntervalTrigger(days=3),
            id='health_check',
            replace_existing=True
        )
        logger.info("INIT: Scheduled health checks every 3 days")
        
        # Initial gap fetch - Non-blocking to ensure fast startup
        logger.info("INIT: Queuing initial SPY gap fetch...")
        asyncio.create_task(refresh_spy_gap())
        
        scheduler.start()
        logger.info("INIT: APScheduler started successfully")
        logger.info(f"INIT: Next scheduled run: {scheduler.get_jobs()[0].next_run_time if scheduler.get_jobs() else 'No jobs'}")
        
    except Exception as e:
        logger.error(f"INIT: Scheduler startup failed: {e}", exc_info=True)

    logger.info("INIT: Application initialization complete, yielding to web server.")
    yield
    
    logger.info("SHUTDOWN: Insider Scanner server shutting down")
    scheduler.shutdown()
    logger.info("SHUTDOWN: Scheduler stopped.")

app = FastAPI(
    title="Insider Scanner",
    description="Scrapes OpenInsider, scores trades, pushes to Airtable + Zapier. Includes Nuxt UI endpoints.",
    version="1.1.0",
    lifespan=lifespan,
)

# Allow Nuxt UI to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import dotenv

@app.get("/")
def health():
    # Simplest possible health check to appease Railway
    # Includes last-run metadata and cached SPY gap for the dashboard.
    return {
        "status": "ok",
        "service": "insider-scanner",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "last_run": _last_run,
        "spy_gap": _cached_spy_gap,
    }

@app.get("/health")
def health_v2():
    # Dedicated health endpoint for monitoring services
    return {"status": "healthy", "uptime": "connected"}


@app.get("/scheduler/status")
def scheduler_status():
    """Check if the automation scheduler is running and when it will next trigger."""
    try:
        jobs = scheduler.get_jobs()
        job_list = []
        for job in jobs:
            next_run = job.next_run_time.isoformat() if job.next_run_time else "Never"
            job_list.append({
                "id": job.id,
                "name": job.name or job.func.__name__,
                "next_run": next_run,
                "trigger": str(job.trigger)
            })
        
        return {
            "scheduler_running": scheduler.running,
            "jobs_count": len(jobs),
            "jobs": job_list,
            "server_time_utc": datetime.now(timezone.utc).isoformat(),
            "timezone_configured": os.getenv("TZ", "Not set"),
        }
    except Exception as e:
        return {
            "scheduler_running": False,
            "error": str(e),
            "server_time_utc": datetime.now(timezone.utc).isoformat()
        }


# Security mapping: only allow modification of non-sensitive strategy settings
EXPOSED_SETTINGS = {
    "MIN_SCORE_FOR_ALERT", 
    "MIN_SCAN_SCORE", 
    "MIN_VOLUME_SHARES",
    "V1_ATR_MIN",
    "V1_VOL_MIN_M",
    "V1_VOL_MAX_M",
    "V2_ATR_MIN",
    "V2_ATR_MAX",
    "V2_VOL_MIN_M",
    "V2_VOL_MAX_M",
    "REPEAT_BUY_DAYS"
}

@app.get("/settings")
def get_settings():
    try:
        # Resolve .env path - prioritize backend/.env where settings usually live
        env_path = backend_dir / ".env"
        if not env_path.exists(): env_path = backend_dir.parent / ".env"
        
        config = dotenv.dotenv_values(env_path)
        
        # Define recommended defaults for missing or blank settings
        defaults = {
            "MIN_SCORE_FOR_ALERT": "85",
            "MIN_SCAN_SCORE": "50",
            "MIN_VOLUME_SHARES": "50000",
            "V1_ATR_MIN": "3.5",
            "V1_VOL_MIN_M": "30",
            "V1_VOL_MAX_M": "100",
            "V2_ATR_MIN": "7.0",
            "V2_ATR_MAX": "20.0",
            "V2_VOL_MIN_M": "30",
            "V2_VOL_MAX_M": "10000",
            "REPEAT_BUY_DAYS": "30"
        }
        
        return {
            k: (config.get(k) or os.getenv(k) or defaults.get(k, "")) 
            for k in EXPOSED_SETTINGS
        }
    except Exception as e:
        logger.error(f"Error fetching settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


from typing import Dict, Any

@app.post("/settings")
def update_settings(payload: Dict[str, Any]):
    try:
        env_path = backend_dir / ".env"
        if not env_path.exists(): env_path = backend_dir.parent / ".env"
        
        logger.info(f"Saving settings to {env_path}")
        
        for key, val in payload.items():
            if key in EXPOSED_SETTINGS:
                # Ensure we have a string value for the .env file
                string_val = str(val)
                dotenv.set_key(str(env_path), key, string_val)
                os.environ[key] = string_val # Update current process memory
                
        return {"status": "success", "message": f"Settings saved to {env_path.name}"}
    except Exception as e:
        logger.error(f"Error saving settings: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})


@app.post("/run")
async def run_pipeline(background_tasks: BackgroundTasks, x_run_secret: str = Header(default="")):
    """
    Trigger the insider scanner pipeline in the background.
    Pass the secret in the header:  X-Run-Secret: <your-secret>
    """
    if RUN_SECRET and x_run_secret != RUN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid or missing X-Run-Secret header")

    if is_pipeline_running:
        return {"ok": False, "message": "Pipeline is already running in the background"}

    background_tasks.add_task(execute_pipeline_core)
    return {"ok": True, "message": "Pipeline started in background"}


@app.get("/run-status")
def get_run_status():
    """Returns the current running state and the live log stream."""
    return {
        "is_running": is_pipeline_running,
        "logs": list(log_queue)
    }


@app.get("/history")
def get_run_history():
    """Returns all historical pipeline runs (sorted by newest first)."""
    hist = load_history()
    # Return all history sorted by newest first
    return list(reversed(hist))


@app.get("/status")
def status():
    return _last_run


@app.post("/health-check")
async def health_check(background_tasks: BackgroundTasks, lookback_days: int = 30, x_run_secret: str = Header(default="")):
    """
    Triggers a 30-day (or custom) 'Strategic Health Check' (backtest) in the background.
    """
    if RUN_SECRET and x_run_secret != RUN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid or missing X-Run-Secret header")

    if is_health_checking:
        return {"ok": False, "message": "Health check is already running in the background"}

    background_tasks.add_task(execute_health_check_core, lookback_days)
    return {"ok": True, "message": f"Strategic Health Check ({lookback_days} days) started in background"}


@app.get("/health-status")
def get_health_status():
    """Returns the current health check state, results, and the live log stream."""
    return {
        "is_running": is_health_checking,
        "results": _last_health_check_results,
        "logs": list(log_queue)
    }

