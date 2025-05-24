import re
from datetime import datetime

import pika
import sqlalchemy as sa
import yt_dlp
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.states import Cancelled, Failed
from prefect.tasks import task_input_hash
from sqlalchemy.orm import Session

from base import Base
from classes.yt_metadata_class import Yt_Metadata

import os
from dotenv import load_dotenv

load_dotenv()


@task
def fetch_messages_from_rmq(queue_name: str, batch_size: int = 10) -> list[str]:
    logger = get_run_logger()
    try:
        rmq_host = os.getenv("RABBITMQ_HOST", "localhost")
        connection = pika.BlockingConnection(pika.ConnectionParameters(rmq_host))
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

# change to prefect secret block

db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise ValueError("DATABASE_URL environment variable is required but not set")
    engine = sa.create_engine(db_url)
    Base.metadata.create_all(engine)

    logger = get_run_logger()

    try:
        with Session(engine) as session:
            session.add(yt_metadata)
            session.commit()
        logger.info("Successfully stored data into DB.")
    except SQLAlchemyError as e:
        logger.error(f"Database operation failed: {e}")
        raise Failed(f"Failed to store metadata: {e}")

@task
def get_info_with_ytdlp(url: str) -> dict:
    logger = get_run_logger()
    
    cookie_file = os.getenv("YT_COOKIE_FILE", "cookies.txt")
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
    
    cookie_file = os.getenv("YT_COOKIE_FILE", "cookies.txt")
    download_dir = os.getenv("YT_DOWNLOAD_DIR", "./downloads")
    
    
    os.makedirs(download_dir, exist_ok=True)
    
    ydl_opts = {
        "format": "bestvideo+bestaudio/best",
        "outtmpl": os.path.join(download_dir, "%(title).100s_%(id)s.%(ext)s"),
        "restrictfilenames": True,  # sanitize filenames
        "windowsfilenames": True    # extra safety for cross-platform
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

    return info


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


if __name__ == "__main__":
    yt_processing_flow()
