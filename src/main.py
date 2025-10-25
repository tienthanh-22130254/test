from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.scheduler.jobs import build_scheduler
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    sched = build_scheduler()
    sched.start()
    yield
    sched.shutdown()

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)
