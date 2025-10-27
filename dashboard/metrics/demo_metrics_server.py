"""
Data‑Ngin Metrics Demo Server
=============================

Purpose
-------
Spin up a tiny FastAPI app on :8003 that:
- Exposes Prometheus metrics at /metrics (pull‑based)
- Lets you trigger simulated pipeline runs at /run (one shot) or /demo-burst
- Emits the six high‑priority metrics you already wired up:
  1) total_pipeline_runs
  2) execution_time_seconds{stage}
  3) errors_total{stage}
  4) records_processed_total
  5) data_completeness (0..1)
  6) last_successful_run_timestamp

Quick start
-----------
1) pip install -U fastapi uvicorn prometheus-client
2) python demo_metrics_server.py
3) In another shell, trigger a run:
   curl -X POST http://localhost:8003/run
   # or fire off 5 runs with varying parameters
   curl -X POST "http://localhost:8003/demo-burst?runs=5"

PromQL ideas
------------
- total_pipeline_runs
- sum by(stage) (rate(execution_time_seconds_count[5m]))  # stage activity
- sum by(stage) (increase(errors_total[15m]))              # recent errors
- rate(records_processed_total[5m])
- data_completeness
- last_successful_run_timestamp

Notes
-----
- This demo keeps metrics in-process. If you already have 
  Prometheus scraping :8003/metrics, it will pick these up immediately.
- Stages simulated: load_symbols -> fetch_data -> clean_data -> insert_data
- Tune behavior via query params or JSON body when POSTing /run.
"""

import asyncio
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import FastAPI, Body, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, CONTENT_TYPE_LATEST
from prometheus_client import generate_latest
from starlette.middleware.cors import CORSMiddleware

# ----------------------------
# Prometheus metrics registry
# ----------------------------
registry = CollectorRegistry()

TOTAL_RUNS = Counter(
    "total_pipeline_runs",
    "Total number of pipeline runs (started).",
    registry=registry,
)

EXEC_TIME = Histogram(
    "execution_time_seconds",
    "Execution time per stage in seconds.",
    labelnames=("stage",),
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60),
    registry=registry,
)

ERRORS = Counter(
    "errors_total",
    "Number of errors per stage (total).",
    labelnames=("stage",),
    registry=registry,
)

RECORDS = Counter(
    "records_processed_total",
    "Total records processed across runs.",
    registry=registry,
)

COMPLETENESS = Gauge(
    "data_completeness",
    "Fraction of expected data received in the last run (0..1).",
    registry=registry,
)

LAST_SUCCESS_TS = Gauge(
    "last_successful_run_timestamp",
    "Unix timestamp of the last fully successful run.",
    registry=registry,
)

# For convenience, export a simple run status gauge (0 idle, 1 running)
RUNNING = Gauge(
    "pipeline_running",
    "Is the pipeline currently executing? (0/1)",
    registry=registry,
)

# ----------------------------
# App init
# ----------------------------
app = FastAPI(title="Data‑Ngin Metrics Demo", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Helpers
# ----------------------------
STAGES: List[str] = [
    "load_symbols",
    "fetch_data",
    "clean_data",
    "insert_data",
]

async def _run_stage(stage: str, mean_s: float, jitter: float, p_error: float) -> None:
    """Simulate running one stage with timing, optional error."""
    start = time.perf_counter()

    # Fake doing work
    sleep_for = max(0.0, random.gauss(mu=mean_s, sigma=jitter))
    await asyncio.sleep(sleep_for)

    # Random error occurrence
    if random.random() < p_error:
        ERRORS.labels(stage=stage).inc()
        # still record time even if error
    elapsed = time.perf_counter() - start
    EXEC_TIME.labels(stage=stage).observe(elapsed)

async def simulate_pipeline_run(
    records_expected: int = 50_000,
    records_actual: Optional[int] = None,
    stage_mean_seconds: Dict[str, float] = None,
    stage_jitter_seconds: Dict[str, float] = None,
    stage_error_rate: Dict[str, float] = None,
) -> Dict[str, float]:
    """Simulate a full pipeline run across all stages and emit metrics."""
    if stage_mean_seconds is None:
        stage_mean_seconds = {
            "load_symbols": 0.25,
            "fetch_data": 1.0,
            "clean_data": 0.7,
            "insert_data": 0.6,
        }
    if stage_jitter_seconds is None:
        stage_jitter_seconds = {s: stage_mean_seconds[s] * 0.25 for s in STAGES}
    if stage_error_rate is None:
        stage_error_rate = {s: 0.02 for s in STAGES}  # 2% default error chance per stage

    TOTAL_RUNS.inc()
    RUNNING.set(1)

    # If not supplied, simulate that we got slightly less than expected 2–8% shortfall sometimes
    if records_actual is None:
        shortfall_frac = max(0.0, random.gauss(0.03, 0.02))  # ~3% avg shortfall
        records_actual = int(records_expected * (1.0 - shortfall_frac))

    # Process stages sequentially
    for s in STAGES:
        await _run_stage(
            s,
            mean_s=stage_mean_seconds.get(s, 0.5),
            jitter=stage_jitter_seconds.get(s, 0.1),
            p_error=stage_error_rate.get(s, 0.0),
        )

    # Records & completeness
    RECORDS.inc(records_actual)
    completeness = 0.0 if records_expected <= 0 else records_actual / float(records_expected)
    COMPLETENESS.set(completeness)

    # If no new errors in this run, mark success timestamp.
    # (Heuristic: success if errors_total didn't increase during this run for any stage.)
    # Simpler approach: if completeness >= 0.98, call it success.
    if completeness >= 0.98:
        LAST_SUCCESS_TS.set(datetime.now(timezone.utc).timestamp())

    RUNNING.set(0)
    return {
        "records_expected": float(records_expected),
        "records_actual": float(records_actual),
        "completeness": completeness,
    }

# ----------------------------
# Routes
# ----------------------------
@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "ok"

@app.get("/metrics")
def metrics():
    # Expose the custom registry we created above
    data = generate_latest(registry)
    return PlainTextResponse(content=data.decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

@app.post("/run")
async def run_once(
    records_expected: int = Query(50_000, ge=0, description="Expected records this run"),
    records_actual: Optional[int] = Query(None, ge=0, description="Actual records ingested. Default: simulated"),
    error_rate_all: float = Query(0.02, ge=0.0, le=1.0, description="Baseline error probability for all stages"),
    faster: bool = Query(False, description="If true, makes all stages faster to help with demos"),
    params: Dict = Body(default=None),
):
    """
    Trigger a single simulated pipeline run.

    You can override per-stage timings/error-rates by sending JSON like:
    {
      "stage_mean_seconds": {"fetch_data": 2.5},
      "stage_error_rate": {"insert_data": 0.10}
    }
    """
    stage_mean_seconds = None
    stage_jitter_seconds = None
    stage_error_rate = None

    if params and isinstance(params, dict):
        stage_mean_seconds = params.get("stage_mean_seconds")
        stage_jitter_seconds = params.get("stage_jitter_seconds")
        stage_error_rate = params.get("stage_error_rate")

    # Apply a simple accel if faster==True
    if faster:
        if stage_mean_seconds is None:
            stage_mean_seconds = {}
        for s in STAGES:
            stage_mean_seconds[s] = stage_mean_seconds.get(s, {"load_symbols":0.1,"fetch_data":0.4,"clean_data":0.3,"insert_data":0.25}.get(s))

    # Default all-stage error rate unless overridden
    if stage_error_rate is None:
        stage_error_rate = {s: error_rate_all for s in STAGES}

    result = await simulate_pipeline_run(
        records_expected=records_expected,
        records_actual=records_actual,
        stage_mean_seconds=stage_mean_seconds,
        stage_jitter_seconds=stage_jitter_seconds,
        stage_error_rate=stage_error_rate,
    )
    return {"status": "ok", "result": result}

@app.post("/demo-burst")
async def demo_burst(
    runs: int = Query(5, ge=1, le=100),
    base_expected: int = Query(40_000, ge=0),
    jitter_frac: float = Query(0.2, ge=0.0, le=1.0, description="How much to vary expected records each run"),
    error_rate_all: float = Query(0.02, ge=0.0, le=1.0),
    faster: bool = Query(True),
):
    """Fire N runs in sequence with slight variation to make graphs move."""
    out = []
    for i in range(runs):
        expected = int(base_expected * max(0.0, random.gauss(1.0, jitter_frac)))
        res = await run_once(
            records_expected=expected,
            records_actual=None,
            error_rate_all=error_rate_all,
            faster=faster,
            params={}
        )
        out.append(res["result"])  # unwrap FastAPI response dict
    return {"fired": runs, "results": out}

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "demo_metrics_server:app",
        host="0.0.0.0",
        port=8003,
        reload=False,
    )
