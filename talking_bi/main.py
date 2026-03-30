from fastapi import FastAPI
from contextlib import asynccontextmanager

from api.upload import router as upload_router
from api.intelligence import router as intelligence_router
from api.run import router as run_router
from services.session_manager import start_cleanup_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the cleanup scheduler
    scheduler = start_cleanup_scheduler()
    print("Session cleanup scheduler started")
    yield
    # Shutdown: Stop the scheduler
    scheduler.shutdown()
    print("Session cleanup scheduler stopped")


app = FastAPI(
    title="Talking BI — Phase 1",
    description="CSV upload, session management, dataset intelligence, and LangGraph pipeline execution",
    version="0.3.0",
    lifespan=lifespan
)

# Include routers
app.include_router(upload_router, tags=["upload"])
app.include_router(intelligence_router, tags=["intelligence"])
app.include_router(run_router, tags=["run"])


@app.get("/")
async def root():
    return {
        "message": "Talking BI API - Phase 1",
        "version": "0.3.0",
        "status": "running"
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}
