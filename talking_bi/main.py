from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager

from api.upload import router as upload_router
from api.intelligence import router as intelligence_router
from api.run import router as run_router
from api.query import router as query_router
from api.metrics import router as metrics_router
from services.session_manager import start_cleanup_scheduler
from auth.routes import router as auth_router
from database import engine, Base
from auth.models import User, Organization  # Ensure models are loaded for create_all

# Create tables
Base.metadata.create_all(bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the cleanup scheduler
    scheduler = start_cleanup_scheduler()
    print("Session cleanup scheduler started")
    yield
    # Shutdown: Stop the scheduler
    scheduler.shutdown()
    print("Session cleanup scheduler stopped")


from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Talking BI — Phase 10",
    description="Product Layer: CSV upload, AI Dataset Profiler, Chat Interface, LLM Pipeline, Analytics & Trace Visibility",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development, allow all. Change to specific frontend origin in production.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Include routers
app.include_router(upload_router, tags=["upload"])
app.include_router(intelligence_router, tags=["intelligence"])
app.include_router(run_router, tags=["run"])
app.include_router(query_router, tags=["query"])
app.include_router(metrics_router, tags=["metrics"])
app.include_router(auth_router)


@app.get("/")
async def root():
    # Return the Chat UI with no-cache headers to ensure JS fixes are loaded
    response = FileResponse("static/index.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/health")
async def health():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
