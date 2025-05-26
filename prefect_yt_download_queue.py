import re
from datetime import datetime

import pika
import sqlalchemy as sa
import yt_dlp
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.states import Cancelled, Failed
from prefect.tasks import task_input_hash
from prefect.blocks.notifications import DiscordWebhook
from prefect.blocks.system import Secret
from prefect_sqlalchemy import SqlAlchemyConnector

from sqlalchemy.orm import Session
import requests
from requests.auth import HTTPBasicAuth

from base import Base
from classes.yt_metadata_class import Yt_Metadata

import os
from dotenv import load_dotenv

load_dotenv()

env_block = Secret.load("spheredefaultenv")
env_data = env_block.get()

@task
def fetch_messages_from_rmq(queue_name: str, batch_size: int = 10) -> list[str]:
    logger = get_run_logger()
    try:
        rmq_host = env_data["RABBITMQ_HOST"]
        rmq_port = env_data["RABBITMQ_PORT"]
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rmq_host, port=rmq_port))
        channel = connection.channel()
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise Failed(f"RabbitMQ connection failed: {e}")

    messages = []

    try:
        for _ in range(batch_size):
            method_frame, header_frame, body = channel.basic_get(queue=queue_name)
            if method_frame:
                messages.append(body.decode())
                channel.basic_ack(method_frame.delivery_tag)
            else:
                break
    finally:
        connection.close()
    
    return messages

@task
def send_notification_to_ntfy_task(video_data: str):
    logger = get_run_logger()
    ntfy_url = env_data["NTFY_URL"]

    if not isinstance(video_data, str):
        logger.warning("Invalid notification data provided")
        return

    try:
        auth = HTTPBasicAuth(username=env_data["HTTPBASICAUTH_USER"], password=env_data["HTTPBASICAUTH_PASSWORD"])
        response = requests.post(f"{ntfy_url}/yt_downloads_prefect", data=video_data, headers={ "Title": "YoutTube Download(s) finished ✔".encode('utf-8') }, timeout=10, auth=auth)
        response.raise_for_status()
        logger.info(f"Notification sent successfully: {response.status_code}")
    except requests.RequestException as e:
        logger.warning(f"Failed to send notification: {e}")


@task
def transform_yt_metadata_task(raw_data: dict) -> dict:
    if not isinstance(raw_data, dict):
        raise TypeError("raw_data must be a dictionary")
    transformed = {
        "title": raw_data.get("title", "Untitled").strip()[:200],
        "duration": str(int(raw_data.get("duration", 0))),
        "uploader": raw_data.get("uploader", "Unknown").strip()[:100],
        "download_date": datetime.utcnow().strftime("%Y-%m-%d"),
        "view_count": str(int(raw_data.get("view_count", 0))),
        "description": raw_data.get("description", "").strip()[:1000],
    }

    logger = get_run_logger()
    upload_date_str = raw_data.get("upload_date", "")
    try:
        upload_date_obj = datetime.strptime(upload_date_str, "%Y%m%d")
        transformed["upload_date"] = upload_date_obj.strftime("%Y-%m-%d")
    except ValueError:
        logger.warning(f"Failed to parse upload_date: {upload_date_str!r}, defaulting to '1970-01-01'")
        transformed["upload_date"] = "1970-01-01"

    return transformed

@task
def create_yt_metadata_instance_task(clean_data: dict):
    return Yt_Metadata(**clean_data)


@task
def store_metadata(yt_metadata: Yt_Metadata):
    from sqlalchemy.exc import SQLAlchemyError

    connector = SqlAlchemyConnector.load("spheredefaultcreds")

    engine = connector.get_engine()
    Base.metadata.create_all(engine)

    logger = get_run_logger()
        
    try:
        with Session(engine) as session:
            session.add(yt_metadata)
            session.expire_on_commit = False
            session.commit()
        logger.info(f"Successfully stored data for {yt_metadata.title} into DB.")
    except SQLAlchemyError as e:
        logger.error(f"Database operation failed: {e}")
        raise Failed(f"Failed to store metadata for {yt_metadata.title}: {e}")

@task
def get_info_with_ytdlp(url: str) -> dict:
    logger = get_run_logger()
    
    cookie_file = env_data["YT_COOKIE_FILE"]
    ydl_opts = {"quiet": True, "skip_download": True}
    
    if os.path.exists(cookie_file):
        ydl_opts['cookiefile'] = cookie_file
    else:
        logger.warning(f"Cookie file not found: {cookie_file}")
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info
    except Exception as e:
        logger.error(f"Failed to extract video info for {url}: {e}")
        raise Failed(f"Video info extraction failed: {e}")

@task
def download_with_ytdlp(url: str):
    logger = get_run_logger()
    
    cookie_file = env_data["YT_COOKIE_FILE"]
    download_dir = env_data["YT_DOWNLOAD_DIR"]
    
    
    os.makedirs(download_dir, exist_ok=True)
    
    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
        "merge_output_format": "mp4",
        "outtmpl": os.path.join(download_dir, "%(title).100s_%(id)s.%(ext)s"),
        "restrictfilenames": True, # sanitize filenames
        "windowsfilenames": True   # extra safety for cross-platform
    }
    
    if os.path.exists(cookie_file):
        ydl_opts['cookiefile'] = cookie_file
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
    except Exception as e:
        logger.error(f"Failed to download video from {url}: {e}")
        raise Failed(f"Video download failed: {e}")

@task(persist_result=True, retries=0, name="Validate URL", cache_key_fn=task_input_hash)
def validate_video_url_task(url: str) -> bool:
    pattern = re.compile(r'^(https?://)?(www\.)?(youtube\.com|youtu\.be|music\.youtube\.com)/.+$')
    if not pattern.match(url):
        raise Failed(f"Invalid YouTube URL format: {url}")
    return True

@flow
def store_video_metadata_subflow(info: dict):
    clean_data = transform_yt_metadata_task(raw_data=info)
    yt_metadata_instance = create_yt_metadata_instance_task(clean_data=clean_data)
    store_metadata(yt_metadata=yt_metadata_instance)

    return yt_metadata_instance

@flow
def download_video_subflow(url: str, yt_metadata_instance: Yt_Metadata):

    logger = get_run_logger()
    try:
        download_with_ytdlp(url)
    except Exception as e:
        logger.error(f"Download failed for {yt_metadata_instance.title} {url}: {e}")
        raise Failed(message=f"Download failed: {e}")
    logger.info(f"Finished downloading video: {yt_metadata_instance.title}")


@flow
def validate_index_subflow(url: str):
    logger = get_run_logger()

    validate_state = validate_video_url_task.submit(url, return_state=True)
    if validate_state.is_failed():
        raise ValueError(f"URL validation failed for {url}")

    info_state = get_info_with_ytdlp.submit(url, return_state=True)
    if info_state.is_failed():
        raise RuntimeError(f"Failed to get info for {url}")

    info = info_state.result()
    logger.info(f"Processing video: {info['title']}")

    yt_metadata_instance = store_video_metadata_subflow(info=info)

    download_state = download_video_subflow(url=url, yt_metadata_instance=yt_metadata_instance, return_state=True)
    if download_state.is_failed():
        raise RuntimeError(f"Download failed for {info['title']} {url}")


@flow
def yt_processing_flow(queue_name: str = "yt_download_requests"):
    logger = get_run_logger()
    messages = fetch_messages_from_rmq(queue_name)

    if not messages:
        logger.info("No videos found in the queue.")
        return Cancelled(message="No videos found in the queue.")

    logger.info(f"Found {len(messages)} videos in the queue.")

    count_succ_links = 0
    count_unsucc_links = 0

    all_results = []
    for url in messages:
        try:
            validate_index_subflow(url=url)
        except Exception as e:
            logger.warning(f"Skipping failed task for URL: {url} due to error: {e}")
            count_unsucc_links += 1
            continue

        logger.info(f"Successfully processed {url}")
        count_succ_links += 1
        all_results.append(url)

    if count_succ_links == 0:
        logger.error(f"Unable to process any videos. Failed to process a total of: {len(messages)}")
        return Failed(message=f"Unable to process any videos. Failed to process a total of: {len(messages)}.")
    logger.info(f"Processing of {len(messages)} video(s) complete. Failed to download {count_unsucc_links}.")

    discord_webhook_block = DiscordWebhook.load("workflowhasfinished")

    send_notification_to_ntfy_task(video_data=f"Processing of {len(messages)} video(s) complete. Failed to download {count_unsucc_links}.")
    discord_webhook_block.notify(f"Finished downloading: {len(messages)} youtube videos.")


if __name__ == "__main__":
    yt_processing_flow()
