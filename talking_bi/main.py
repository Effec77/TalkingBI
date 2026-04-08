from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import os

from api.upload import router as upload_router
from api.intelligence import router as intelligence_router
from api.run import router as run_router
from api.query import router as query_router
from api.metrics import router as metrics_router
from services.session_manager import start_cleanup_scheduler
from auth.routes import router as auth_router
from database import engine, Base
from auth.models import User, Organization, UserAPIKey, AuthActivityLog, ensure_auth_schema  # Ensure models are loaded for create_all

# Create tables
Base.metadata.create_all(bind=engine)
ensure_auth_schema(engine)


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
# NOTE:
# `allow_credentials=True` cannot be used with wildcard origin `*`.
# Build an explicit allow-list from env so deployed frontend can call backend.
frontend_url = os.getenv("FRONTEND_URL", "http://127.0.0.1:5173").rstrip("/")
extra_origins_raw = os.getenv("CORS_ALLOWED_ORIGINS", "")
extra_origins = [o.strip().rstrip("/") for o in extra_origins_raw.split(",") if o.strip()]
allow_origins = list(dict.fromkeys([frontend_url, "http://localhost:5173", "http://127.0.0.1:5173", *extra_origins]))
# Allow common hosted frontend origins when credentials are enabled:
# - Vercel production/preview domains
# - AWS S3 static website endpoints (http)
allow_origin_regex = os.getenv(
    "CORS_ALLOW_ORIGIN_REGEX",
    r"(^https://([a-z0-9-]+\.)?vercel\.app$)|(^http://[a-z0-9.-]+\.s3-website\.[a-z0-9-]+\.amazonaws\.com$)",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_origin_regex=allow_origin_regex,
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
