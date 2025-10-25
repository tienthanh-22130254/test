from fastapi import APIRouter
from src.service.controller_service.crawl_controller import CrawlController
from src.dao.db import log_event

router = APIRouter(prefix="/crawl", tags=["crawl"])
_controller = CrawlController()

@router.get("/health")
async def crawl_health():
    return {"status": "ok", "service": "crawl"}

@router.post("/run")
async def run_now():
    log_event("INFO", "manual crawl trigger", {})
    result = await _controller.run()
    return {"status": "started", "processed": len(result)}
