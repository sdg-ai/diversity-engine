import pytest
import logging
import os
from core.wasabi import WASABI_CONNECT

logger = logging.getLogger(__name__)

SERVICE_ENDPOINT = os.getenv(
    "SERVICE_ENDPOINT", "https://s3.us-west-1.wasabisys.com")
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")


@pytest.mark.asyncio
async def test_one_scorecard():
    WASABI = WASABI_CONNECT(
        service_endpoint=SERVICE_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
    )
    scorecard = WASABI.object_content(
        object_path="company_scorecards/scorecards.json")
    logger.info(scorecard)
    return scorecard
