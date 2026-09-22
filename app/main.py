from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.core.config import settings
from app.api.v1.message import router as message_router
from app.channels import router as channels_router
from app.db.pool import db_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure the asyncpg pool exists before serving (mirrors n8n's warm Postgres credential).
    try:
        await db_pool.get_pool()
    except Exception as exc:
        print(f"[Warning] Could not initialize DB pool at startup: {exc}")
    yield
    await db_pool.close()


app = FastAPI(
    title=settings.APP_NAME,
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.include_router(message_router)
app.include_router(channels_router)


@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "app": settings.APP_NAME, "version": "2.0.0"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host=settings.HOST, port=settings.PORT, reload=settings.DEBUG)
