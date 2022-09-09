from fastapi import (
    # Depends,
    FastAPI,
    # File,
    # HTTPException,
    # Security,
    # UploadFile,
)
from fastapi.routing import APIRouter
import os
from core.wasabi import WASABI_CONNECT

SERVICE_ENDPOINT = os.getenv(
    "SERVICE_ENDPOINT", "https://s3.us-west-1.wasabisys.com")
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")

app = FastAPI(
    title="Diversity API",
    description="Diversity project API",
    version="0.0.1",
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
    openapi_url="/api/v1/openapi.json",
)
router = APIRouter()


@router.get("/")
def root():
    """ """
    return {"Hello": "Test diversity engine"}


@router.get("/company_scorecard")
async def get_company_scorecard(
    company_id: str
):
    """ """
    WASABI = WASABI_CONNECT(
        service_endpoint=SERVICE_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
    )
    scorecards = WASABI.object_content(
        object_path="company_scorecards/scorecards.json")
    return scorecards

app.include_router(router, prefix="/api/v1")
