import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

from src.config.setting import get_settings
from src.service.controller_service.crawl_controller import CrawlController
from src.dao.db import log_event

logger = logging.getLogger(__name__)

def build_scheduler() -> AsyncIOScheduler:
    settings = get_settings()
    jobstores = {"default": SQLAlchemyJobStore(url=settings.resolved_db_url())}
    scheduler = AsyncIOScheduler(timezone=settings.scheduler_timezone, jobstores=jobstores)
    controller = CrawlController()

    async def crawl_job():
        logger.info("crawl.run", extra={"args": {"proc": "crawl.run"}})
        log_event("INFO", "scheduled_crawl_started", {})
        await controller.run()
        log_event("INFO", "scheduled_crawl_finished", {})

    async def load_staging_job():
        logger.info("load_staging.run", extra={"args": {"proc": "load_staging.run"}})
        log_event("INFO", "load_staging_run", {})

    def job_listener(event):
        if event.exception:
            logger.error(
                "scheduler.job_error",
                extra={
                    "args": {
                        "proc": "scheduler.job_error",
                        "run_id": getattr(event, "job_id", None),
                        "err_stack": getattr(event, "traceback", None),
                    }
                },
                exc_info=False,
            )
            try:
                log_event("ERROR", "job_error", {"job_id": getattr(event, "job_id", None)})
            except Exception:
                pass
        elif event.code == EVENT_JOB_MISSED:
            logger.warning(
                "scheduler.job_missed",
                extra={"args": {"proc": "scheduler.job_missed", "run_id": getattr(event, "job_id", None)}},
            )

    scheduler.add_job(
        crawl_job,
        IntervalTrigger(minutes=1),
        id="crawl_job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )
    scheduler.add_job(
        load_staging_job,
        CronTrigger(minute=0),
        id="load_staging_job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)
    return scheduler
