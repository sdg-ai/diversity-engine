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
import json
import boto3
import gzip
# from core.wasabi import WASABI_CONNECT
from fastapi.middleware.cors import CORSMiddleware
# from core.s3_utilities import read_jsonl_file

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@router.get("/")
def root():
    """ """
    return {"Hello": "Test diversity engine"}


# @router.get("/company_scorecard")
# async def get_company_scorecard(
#    company_id: str
# ):
#    """ """
#    WASABI = WASABI_CONNECT(
#        service_endpoint=SERVICE_ENDPOINT,
#        access_key_id=ACCESS_KEY_ID,
#        secret_access_key=SECRET_ACCESS_KEY,
#    )
#    scorecards = WASABI.object_content(
#        object_path="company_scorecards/scorecards.json")
#    return scorecards

def read_jsonl_file(
    service_endpoint: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str = "dei-bucket",
    object_path: str = "raw_data/ADM.jsonl.gz",
    return_lines: bool = False,
):
    """
    reads the contents of a JSON Lines file stored in a Wasabi bucket
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      endpoint_url=service_endpoint)

    response = s3.get_object(Bucket=bucket_name, Key=object_path)

    # read the contents of the file
    file_content = response['Body'].read()

    # decompress the gzipped content
    file_content = gzip.decompress(file_content)

    # parse the JSON lines
    json_lines = file_content.decode('utf-8').split('\n')
    if return_lines:
        return json_lines
    else:
        json_objects = []
        for line in json_lines:
            if line:
                json_objects.append(json.loads(line))

        return json_objects


def get_all_company_data():
    all_dat = read_jsonl_file(
        service_endpoint=SERVICE_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
        bucket_name="dei-bucket",
        object_path="company_scores/all/all_companies.jsonl.gz",
        return_lines=True
    )
    result_dict = {}
    for d in all_dat:
        result_dict.update(json.loads(d))
    return result_dict


def get_all_companies():
    """
    get the metadata about companies:
    - company name
    - industry
    - sector
    - company size
    - company website
    - company id
    """
    all_dat = read_jsonl_file(
        service_endpoint=SERVICE_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
        bucket_name="dei-bucket",
        object_path="company_scores/all/companies_metadata.jsonl.gz",
        return_lines=True
    )
    result_dict = {}
    for d in all_dat:
        result_dict.update(json.loads(d))
    return result_dict


def get_one_company_data(company_id: str):
    all_dat = read_jsonl_file(
        service_endpoint=SERVICE_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
        bucket_name="dei-bucket",
        object_path=f"company_scores/by_company_id/{company_id}.jsonl.gz",
        return_lines=True
    )
    company_dict = {}
    for d in all_dat:
        d_format = json.loads(d)
        for k, item in d_format.items():
            company_dict[k] = item
    return company_dict


def get_industry_stats():
    all_dat = read_jsonl_file(
        service_endpoint=SERVICE_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
        bucket_name="dei-bucket",
        object_path="company_scores/all/industry_statistics.jsonl.gz",
        return_lines=True
    )
    result_dict = {}
    for d in all_dat:
        result_dict.update(json.loads(d))
    return result_dict


def get_sector_stats():
    all_dat = read_jsonl_file(
        service_endpoint=SERVICE_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
        bucket_name="dei-bucket",
        object_path="company_scores/all/sector_statistics.jsonl.gz",
        return_lines=True
    )
    result_dict = {}
    for d in all_dat:
        result_dict.update(json.loads(d))
    return result_dict


@router.get("/all_company_data")
async def get_all_data():
    data = get_all_company_data()
    return data


@router.get("/all_companies")
async def get_data():
    data = get_all_companies()
    return data


@router.get("/company")
async def get_one_company(
    company_id: str,
):
    data = get_one_company_data(company_id=company_id)
    return data


@router.get("/industry_stats")
async def get_data():
    data = get_industry_stats()
    return data


@router.get("/sector_stats")
async def get_data():
    data = get_sector_stats()
    return data

app.include_router(router, prefix="/api/v1")
