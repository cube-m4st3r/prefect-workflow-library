import prefect
from prefect import flow, task
import prefect.context
from prefect.states import Cancelled
from prefect.logging import get_run_logger
import sqlalchemy as sa
from sqlalchemy.orm import Session
import requests
from prefect.blocks.system import Secret
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import datetime

from requests.auth import HTTPBasicAuth
from classes.apod_class import Apod
from base import Base

env_block = Secret.load("spheredefaultenv")
env_data = env_block.get()


api_key = env_data["NASA_API_KEY"]
if not api_key:
    raise ValueError("NASA_API_KEY environment variable is not set")
url = "https://api.nasa.gov/planetary/apod"
params = {
    "api_key": api_key
}


@task
def send_notification_to_ntfy_task(data: str):
    logger = get_run_logger()
    
    ntfy_url = env_data["NTFY_URL"]
    
    if not data or not isinstance(data, str):
        logger.warning("Invalid notification data provided")
        return
    
    try:
        auth = HTTPBasicAuth(username=env_data["HTTPBASICAUTH_USER"], password=env_data["HTTPBASICAUTH_PASSWORD"])
        response = requests.post(f"{ntfy_url}/nasa_apod_prefect", data=data, timeout=10, auth=auth)
        response.raise_for_status()
        logger.info(f"Notification sent successfully: {response.status_code}")
    except requests.RequestException as e:
        logger.warning(f"Failed to send notification: {e}")
        return Cancelled(message=f"Failed to send notification: {e}")
        

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
    
    connector = SqlAlchemyConnector.load("spheredefaultcreds")
    engine = connector.get_engine()
    Base.metadata.create_all(engine)

    logger = get_run_logger()

    with Session(engine) as session:
        logger.info(f"checking if dataset for date: '{apod.date}' exists in database")
        if session.query(Apod).filter(Apod.date == apod.date).first():
            logger.info(f"APOD for date {apod.date} already exists in DB.")
            raise ValueError(f"APOD data already exists for this date: {apod.date}")
                    
        session.add(apod)
        session.expire_on_commit = False
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