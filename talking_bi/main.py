from fastapi import FastAPI
from contextlib import asynccontextmanager

from api.upload import router as upload_router
from api.intelligence import router as intelligence_router
from api.run import router as run_router
from api.query import router as query_router
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
    title="Talking BI — Phase 6",
    description="CSV upload, session management, dataset intelligence, LangGraph pipeline execution, and conversation-based queries",
    version="0.4.0",
    lifespan=lifespan,
)

# Include routers
app.include_router(upload_router, tags=["upload"])
app.include_router(intelligence_router, tags=["intelligence"])
app.include_router(run_router, tags=["run"])
app.include_router(query_router, tags=["query"])


@app.get("/")
async def root():
    return {
        "message": "Talking BI API - Phase 6",
        "version": "0.4.0",
        "status": "running",
        "features": ["upload", "intelligence", "run", "query"],
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
