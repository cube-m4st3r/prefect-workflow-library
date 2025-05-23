import prefect
from prefect import flow, task
import prefect.context
from prefect.states import Cancelled
from prefect.logging import get_run_logger
import sqlalchemy as sa
from sqlalchemy.orm import Session
import requests
from datetime import datetime

from classes.apod_class import Apod
from base import Base

import os

api_key = os.getenv("NASA_API_KEY")
if not api_key:
    raise ValueError("NASA_API_KEY environment variable is not set")
url = "https://api.nasa.gov/planetary/apod"
params = {
    "api_key": api_key
}


@task
def api_request_task():
    logger = get_run_logger()
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise


@task
def transform_apod_data_task(raw_data: dict) -> dict:
    transformed = {
        "title": raw_data.get("title", "Untitled").strip(),
        "explanation": raw_data.get("explanation", "").strip()[:1000],
        "media_type": raw_data.get("media_type", "unknown"),
        "hdurl": raw_data.get("hdurl"),
        "url": raw_data.get("url", ""),
        "service_version": raw_data.get("service_version", "v1"),
        "copyright": raw_data.get("copyright", "NASA"),
    }

    date_str = raw_data.get("date", "")
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        transformed["date"] = date_obj.strftime("%Y-%m-%d")
    except ValueError:
        transformed["date"] = "1970-01-01"

    return transformed


@task
def create_apod_instance_task(clean_data: dict):
    return Apod(**clean_data)


@task
def store_apod_data_task(apod: Apod):
    import os
    db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:pass@localhost:5432/db")
    engine = sa.create_engine(db_url)
    Base.metadata.create_all(engine)

    logger = get_run_logger()

    with Session(engine) as session:
        logger.info(f"checking if dataset for date: '{apod.date}' exists in database")
        if session.query(Apod).filter(Apod.date == apod.date).first():
            logger.info(f"APOD for date {apod.date} already exists in DB.")
            raise ValueError(f"APOD data already exists for this date: {apod.date}")
                    
        session.add(apod)
        session.commit()

    logger.info("Successfully stored data into DB.")


@flow(name="Get NASA APOD and Store in DB")
def get_apod_flow():
    raw_data = api_request_task()
    clean_data = transform_apod_data_task(raw_data)
    apod_instance = create_apod_instance_task(clean_data)
    
    store_apod_data_task(apod_instance)
    
    print(prefect.context.get_run_context().flow_run.id)


if __name__ == "__main__":
    get_apod_flow()