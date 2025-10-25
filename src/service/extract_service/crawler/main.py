import uvicorn
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from src.config.setting import SERVER_HOST, SERVER_PORT
from src.service.controller_service.crawl_controller import CrawlController
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(
    jobstores={'default': MemoryJobStore()},
    executors={'default': ThreadPoolExecutor(max_workers=5)},
    job_defaults={
        'coalesce': True,
        'max_instances': 1,
        'misfire_grace_time': 300
    }
)

crawl_controller = CrawlController()

def job_listener(event):
    if event.exception:
        logger.error(f"Job {event.job_id} failed: {event.exception}")
    else:
        logger.info(f"Job {event.job_id} executed successfully")

def crawl_data():
    try:
        logger.info("Starting crawl_data job")
        crawl_controller.get_config()
        logger.info("Completed crawl_data job")
    except Exception as e:
        logger.error(f"Error in crawl_data: {e}", exc_info=True)
        raise

def insert_new_log_crawler_daily():
    try:
        logger.info("Starting insert_new_log_crawler_daily job")
        crawl_controller.call_controller_procedure('insert_new_log_crawler_daily', ())
        logger.info("Completed insert_new_log_crawler_daily job")
    except Exception as e:
        logger.error(f"Error in insert_new_log_crawler_daily: {e}", exc_info=True)
        raise

def load_data_from_file_to_staging():
    pass

def transforms_data():
    pass

def load_data_from_staging_to_warehouse():
    pass

def load_data_from_warehouse_to_data_mart():
    pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application...")
    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.start()

    scheduler.add_job(
        insert_new_log_crawler_daily,
        CronTrigger(hour=8, minute=0),
        id='insert_new_log_crawler_daily',
        replace_existing=True
    )
    scheduler.add_job(
        crawl_data,
        IntervalTrigger(minutes=10),
        id='crawl_data',
        replace_existing=True
    )
    scheduler.add_job(
        load_data_from_file_to_staging,
        IntervalTrigger(minutes=20),
        id='load_data_from_file_to_staging',
        replace_existing=True
    )
    scheduler.add_job(
        transforms_data,
        IntervalTrigger(minutes=20),
        id='transforms_data',
        replace_existing=True
    )
    scheduler.add_job(
        load_data_from_staging_to_warehouse,
        IntervalTrigger(minutes=20),
        id='load_data_from_staging_to_warehouse',
        replace_existing=True
    )
    scheduler.add_job(
        load_data_from_warehouse_to_data_mart,
        IntervalTrigger(minutes=20),
        id='load_data_from_warehouse_to_data_mart',
        replace_existing=True
    )

    logger.info("Scheduler started with all jobs")
    yield

    logger.info("Shutting down application...")
    scheduler.shutdown(wait=True)
    logger.info("Scheduler shutdown complete")

app = FastAPI(lifespan=lifespan)

ALLOWED_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8080").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "scheduler": scheduler.running,
        "jobs": len(scheduler.get_jobs())
    }

@app.get("/")
async def root():
    return {"message": "Crawler API is running"}

if __name__ == '__main__':
    uvicorn.run(
        "src.main:app",
        host=SERVER_HOST,
        port=SERVER_PORT,
        reload=True
    )
