import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, JobExecutionEvent

from src.config.setting import (
    CONTROLLER_DB_HOST,
    CONTROLLER_DB_PORT,
    CONTROLLER_DB_USER,
    CONTROLLER_DB_PASS,
    CONTROLLER_DB_NAME,
)
from src.service.controller_service.crawl_controller import CrawlController
from src.service.controller_service.load_staging_controller import LoadStagingController
from src.config import procedure as proc

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

# Global scheduler instance
_scheduler: Optional[BackgroundScheduler] = None


def _build_jobstore_url() -> str:
    """
    Build SQLAlchemy URL for APScheduler JobStore.
    Priority:
      1) SCHEDULER_DB_URL (env) - helpful for tests or custom URLs, e.g., sqlite:///:memory:
      2) MySQL via mysql+mysqlconnector from .env settings
    """
    override = os.getenv("SCHEDULER_DB_URL")
    if override:
        return override
    # mysql-connector-python supports the mysql+mysqlconnector dialect
    user = CONTROLLER_DB_USER or ""
    pwd = CONTROLLER_DB_PASS or ""
    host = CONTROLLER_DB_HOST or "localhost"
    port = CONTROLLER_DB_PORT or 3306
    db = CONTROLLER_DB_NAME or "controller"
    return f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"


def create_scheduler() -> BackgroundScheduler:
    """
    Create a BackgroundScheduler with:
      - SQLAlchemy jobstore (persistent)
      - ThreadPool executors (10 & 20)
      - Job defaults per requirements
    """
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    jobstore_url = _build_jobstore_url()
    scheduler = BackgroundScheduler(
        jobstores={"default": SQLAlchemyJobStore(url=jobstore_url)},
        executors={
            "default": ThreadPoolExecutor(max_workers=10),
            "heavy": ThreadPoolExecutor(max_workers=20),
        },
        job_defaults={
            "max_instances": 1,
            "misfire_grace_time": 120,
            "coalesce": True,
        },
        timezone="UTC",
    )

    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)
    _scheduler = scheduler
    return scheduler


def get_scheduler() -> Optional[BackgroundScheduler]:
    return _scheduler


def start_scheduler() -> BackgroundScheduler:
    scheduler = create_scheduler()
    if not scheduler.running:
        scheduler.start()
        logger.info("BackgroundScheduler started")
    return scheduler


def shutdown_scheduler(wait: bool = True) -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=wait)
        logger.info("BackgroundScheduler shutdown complete")


def job_error_listener(event: JobExecutionEvent) -> None:
    """
    Listener for JOB_ERROR events. Logs traceback + run_id + source_id if available.
    """
    try:
        run_id = getattr(event.exception, "run_id", None)
        source_id = getattr(event.exception, "source_id", None)
    except Exception:
        run_id = None
        source_id = None

    logger.error(
        "JOB ERROR | job_id=%s | scheduled=%s | run_id=%s | source_id=%s | exc=%s",
        event.job_id,
        getattr(event, "scheduled_run_time", None),
        run_id if run_id else "N/A",
        source_id if source_id else "N/A",
        repr(event.exception),
    )
    if event.traceback:
        logger.error("Traceback:\n%s", event.traceback)


def run_with_retry(
    func: Callable[[], None],
    *,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: float = 0.0,
    context: Optional[dict] = None,
) -> None:
    """
    Simple exponential backoff retry wrapper.
    """
    attempt = 0
    while True:
        try:
            return func()
        except Exception as exc:
            attempt += 1
            ctx = context or {}
            run_id = ctx.get("run_id")
            source_id = ctx.get("source_id")
            logger.warning(
                "Retry attempt %d/%d failed | run_id=%s | source_id=%s | error=%s",
                attempt,
                max_retries,
                run_id if run_id else "N/A",
                source_id if source_id else "N/A",
                repr(exc),
            )
            if attempt >= max_retries:
                # Attach context for listener
                setattr(exc, "run_id", run_id)
                setattr(exc, "source_id", source_id)
                logger.error(
                    "Max retries reached | run_id=%s | source_id=%s", run_id, source_id
                )
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1))) + jitter
            time.sleep(delay)


def crawl_job(throttle_seconds: float = 2.0) -> None:
    """
    Every 1 minute. Fetch config(s) and run crawler(s) with retry/backoff/throttle.
    Notes:
    - Current repo's CrawlController.get_config() already encapsulates fetching a single config
      and executing a crawl. If that method returns/handles a list in your DB logic, the loop below
      will iterate sources accordingly. Otherwise, it still runs one cycle per minute.
    """
    controller = CrawlController()

    # Fetch one or many crawl tasks via controller.get_config()
    # If your procedure returns a list of sources/configs, iterate them here.
    # For now, we execute controller.get_config() once as it internally runs the crawl.
    sources = [None]  # Placeholder for loop structure

    for idx, source in enumerate(sources):
        run_id = str(uuid.uuid4())
        source_id = getattr(source, "id", None) if source else None
        logger.info(
            "crawl_job started | run_id=%s | source_id=%s | idx=%d", run_id, source_id, idx
        )

        def _do():
            # If you have per-source execution, pass necessary args here
            controller.get_config()

        run_with_retry(
            _do,
            max_retries=3,
            base_delay=1.0,
            max_delay=15.0,
            jitter=0.0,
            context={"run_id": run_id, "source_id": source_id},
        )

        logger.info(
            "crawl_job completed | run_id=%s | source_id=%s | idx=%d",
            run_id,
            source_id,
            idx,
        )
        # Throttle between sources
        if throttle_seconds and idx < len(sources) - 1:
            time.sleep(throttle_seconds)


def load_staging_job() -> None:
    """
    Every 1 hour. Calls warehouse staging loader procedures per source.
    """
    controller = LoadStagingController()

    # Map sources to their corresponding stored procedures
    procedures = [
        proc.load_staging_warehouse_batdongsan_com_vn,
        proc.load_staging_warehouse_muaban_net,
    ]

    for name in procedures:
        run_id = str(uuid.uuid4())
        source_id = name  # using procedure name as a proxy for source_id
        logger.info("load_staging_job start | run_id=%s | source_id=%s", run_id, source_id)

        def _do():
            controller.call_controller_procedure(name, ())

        run_with_retry(
            _do,
            max_retries=3,
            base_delay=1.0,
            max_delay=15.0,
            jitter=0.0,
            context={"run_id": run_id, "source_id": source_id},
        )

        logger.info("load_staging_job done | run_id=%s | source_id=%s", run_id, source_id)


def schedule_jobs(scheduler: Optional[BackgroundScheduler] = None) -> BackgroundScheduler:
    """
    Register the two jobs with their intervals.
    """
    sched = scheduler or create_scheduler()

    # 1) crawl_job: every 1 minute
    sched.add_job(
        crawl_job,
        trigger=IntervalTrigger(minutes=1),
        id="crawl_job",
        replace_existing=True,
        executor="default",
    )

    # 2) load_staging_job: every 1 hour
    sched.add_job(
        load_staging_job,
        trigger=IntervalTrigger(hours=1),
        id="load_staging_job",
        replace_existing=True,
        executor="heavy",
    )
    logger.info("Jobs scheduled: %s", [job.id for job in sched.get_jobs()])
    return sched


@asynccontextmanager
async def lifespan(app):
    """
    FastAPI lifespan to ensure scheduler is started and gracefully shut down.
    Usage in FastAPI app:
        app = FastAPI(lifespan=lifespan)
    """
    # Start
    scheduler = start_scheduler()
    schedule_jobs(scheduler)
    logger.info("Scheduler started and jobs registered")
    try:
        yield
    finally:
        # Graceful shutdown
        shutdown_scheduler(wait=True)