from prefect import flow, task
from prefect.states import Cancelled
from prefect.logging import get_run_logger
import sqlalchemy as sa
from sqlalchemy.orm import Session
import requests
from prefect.blocks.system import Secret
from prefect.blocks.notifications import DiscordWebhook
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import datetime
from discord import Embed
from utils.image_manipulation import image_manipulation as IM
from handlers.loki_logging import get_logger
import httpx
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

loki_logger = get_logger(
    "sphere.automation.prefect",
    level="debug",
    labels={
        "app": "sphere",
        "env": "dev",
        "service": "prefect",
        "lang": "python",
        "script": "prefect_nasa_apod.py"
    }
)

@task
def send_notification_to_ntfy_task(apod: Apod):
    logger = get_run_logger()
    
    ntfy_url = env_data["NTFY_URL"]
    
    data=f"Added APOD post: {apod.title} for {apod.date}"
    
    if not data or not isinstance(data, str):
        logger.warning("Invalid notification data provided")
        return
    
    try:
        auth = HTTPBasicAuth(username=env_data["HTTPBASICAUTH_USER"], password=env_data["HTTPBASICAUTH_PASSWORD"])
        response = requests.post(f"{ntfy_url}/nasa_apod_prefect", data=data, timeout=10, auth=auth)
        response.raise_for_status()
        logger.info(f"Notification sent successfully: {response.status_code}")
        loki_logger.info(f"Notification sent successfully: {response.status_code}")
        
    except requests.RequestException as e:
        logger.warning(f"Failed to send notification: {e}")
        loki_logger.warning(f"Failed to send notification: {e}")
        return Cancelled(message=f"Failed to send notification: {e}")

@task
def create_embed_task(apod: Apod):
    """Task to create the APOD embed"""
    """
        Notes: need to fix the image loading, discord is having trouble to fetch the data from the original nasa.gov url
                other than that, the data is being displayed correctly.
    """
    logger = get_run_logger()
    try:
        apod_embed = Embed()
        
        #image_data = apod.url.read()
        
        #hex_color = IM._get_highest_frequency_color_hex(image_data=image_data)
        
        apod_embed.title = apod.title
        apod_embed.description = apod.explanation
        if apod.copyright:
            apod_embed.set_footer(text=f"Copyright: {apod.copyright} | {apod.date}")
        apod_embed.set_footer(text=f"{apod.date}")
        #apod_embed.set_image(url="https://apod.nasa.gov/apod/image/2505/RhoZeta_Nowak_2560.jpg")
        #apod_embed.color = int(hex_color.replace("#", ""), 16)
        
        logger.info(f"Created APOD Embed")
        loki_logger.info(f"Created APOD Embed")
        
        return apod_embed.to_dict()

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        loki_logger.error(f"An unexpected error occurred: {e}")
        return None
    
    
@task
def post_apod_embed(embed: Embed):
    logger = get_run_logger()
    # discord_webhook_block = DiscordWebhook.load("nasaapodpost")
    # discord_webhook_block.notify(embed=embed)
    
    webhook_url = "https://discord.com/api/webhooks/1378686035856457778/RLpRNUWM9rjtBsMlB3seK-eNezoElYv1bTw_CFun1g8ocucN3oOrolgkzCKd0vt-BUgd"
    
    payload = {
        "username": "NASA APOD POST",
        "embeds": [embed]
    }
    
    response = httpx.post(webhook_url, json=payload)
    response.raise_for_status() 
    
    logger.info("Successfully sent APOD Embed notification via webhook")
    loki_logger.info("Successfully sent APOD Embed notification via webhook")
    

@task
def api_request_task():
    logger = get_run_logger()
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logger.info(f"API request was successful. Status code: {response.status_code}")
        loki_logger.info(f"API request was successful. Status code: {response.status_code}")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        loki_logger.error(f"API request failed: {e}")
        raise


@task
def transform_apod_data_task(raw_data: dict) -> dict:
    logger = get_run_logger()
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

    logger.info(f"Successfully transformed data.")
    loki_logger.info(f"Successfully transformed data.")

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
            loki_logger.info(f"APOD for date {apod.date} already exists in DB.")
            raise ValueError(f"APOD data already exists for this date: {apod.date}")
                    
        session.add(apod)
        session.expire_on_commit = False
        session.commit()

    logger.info("Successfully stored data into DB.")
    loki_logger.info("Successfully stored data into DB.")


@task
def send_discord_notification_task(apod: Apod):
    logger = get_run_logger()
    discord_webhook_block = DiscordWebhook.load("workflowhasfinished")
    discord_webhook_block.notify(f"`[APOD] Data processed: '{apod.title}' | {apod.date}`")
    
    logger.info("Successfully sent workflow-finished notification via webhook")
    loki_logger.info("Successfully sent workflow-finished notification via webhook")


@flow(name="Post APOD data")
def post_apod_data_flow(apod: Apod):
    embed = create_embed_task(apod=apod)
    post_apod_embed(embed=embed)


@flow(name="Send out notifications")
def send_out_notifications_flow(apod: Apod):
    send_notification_to_ntfy_task(apod=apod)
    send_discord_notification_task(apod=apod)


@flow(name="Get NASA APOD and Store in DB")
def get_apod_flow():
    raw_data = api_request_task()
    clean_data = transform_apod_data_task(raw_data)
    apod_instance = create_apod_instance_task(clean_data)
    
    store_apod_data_task(apod_instance)
    
    send_out_notifications_flow(apod=apod_instance)
    post_apod_data_flow(apod=apod_instance)


if __name__ == "__main__":
    get_apod_flow()