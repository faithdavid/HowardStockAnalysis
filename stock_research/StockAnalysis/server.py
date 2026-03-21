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
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

RUN_SECRET = os.getenv("RUN_SECRET", "")  # set this in production!

# In-memory last-run state
_last_run: dict = {"status": "never", "time": None, "signals": 0, "message": ""}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Insider Scanner server starting up")
    yield
    logger.info("Insider Scanner server shutting down")


app = FastAPI(
    title="Insider Scanner",
    description="Scrapes OpenInsider, scores trades, pushes to Airtable + Zapier",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
def health():
    return {
        "status": "ok",
        "service": "insider-scanner",
        "last_run": _last_run,
    }


@app.post("/run")
def run_pipeline(x_run_secret: str = Header(default="")):
    """
    Trigger the insider scanner pipeline.
    Pass the secret in the header:  X-Run-Secret: <your-secret>
    """
    if RUN_SECRET and x_run_secret != RUN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid or missing X-Run-Secret header")

    logger.info("Pipeline triggered via /run endpoint")

    try:
        from main import run
        signals = run() or []

        _last_run["status"] = "completed"
        _last_run["time"] = datetime.now(timezone.utc).isoformat()
        _last_run["signals"] = len(signals)
        _last_run["message"] = f"{len(signals)} signals generated"

        return JSONResponse({
            "ok": True,
            "signals_found": len(signals),
            "signals": [
                {
                    "ticker":      s["ticker"],
                    "score":       s["total_score"],
                    "rating":      s.get("rating", "N/A"),
                    "variant":     s.get("variant", "Technical"),
                    "entry":       s["entry_price"],
                    "stop":        s["stop_loss"],
                    "take_profit": s["take_profit"],
                    "rationale":   s["rationale"],
                }
                for s in signals
            ],
        })

    except Exception as e:
        _last_run["status"] = "error"
        _last_run["time"] = datetime.now(timezone.utc).isoformat()
        _last_run["message"] = str(e)
        logger.exception("Pipeline failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
def status():
    return _last_run
