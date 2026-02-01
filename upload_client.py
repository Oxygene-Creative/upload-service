import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

DEFAULT_TIMEOUT = 10800  # 3 hours for long operations like uploads/downloads

logger = logging.getLogger(__name__)


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(name, default)
    if value is None:
        return None
    return value.strip()


def get_mediaserver_uri(explicit_uri: Optional[str] = None) -> str:
    uri = explicit_uri or _get_env("MEDIASERVER_URI")
    if not uri:
        raise ValueError("MEDIASERVER_URI is required for uploads")
    return uri


def get_audiovisual_api_uri(explicit_uri: Optional[str] = None) -> Optional[str]:
    uri = explicit_uri or _get_env("AUDIOVISUAL_API_URI")
    return uri or None


def get_ingestion_bucket(explicit_bucket: Optional[str] = None) -> str:
    return explicit_bucket or _get_env("INGESTION_BUCKET", "audiovisual-streams")


def get_ingestion_delay_seconds(
    explicit_delay: Optional[float] = None,
) -> float:
    if explicit_delay is not None:
        return explicit_delay
    raw = _get_env("INGESTION_TRIGGER_DELAY_SECONDS", "0")
    try:
        return float(raw or 0)
    except ValueError as exc:
        raise ValueError(
            "INGESTION_TRIGGER_DELAY_SECONDS must be a numeric value"
        ) from exc


async def upload_recording(
    file_path: str,
    stream_id: str,
    stream_name: str,
    timestamp: str,
    mediaserver_uri: Optional[str] = None,
) -> dict:
    """Uploads the recording file to the media server."""
    file_name = Path(file_path).name
    logger.info(
        "Attempting to upload file: %s for stream %s...",
        file_name,
        stream_name,
    )

    uri = get_mediaserver_uri(mediaserver_uri)

    async with httpx.AsyncClient(
        timeout=DEFAULT_TIMEOUT,
        verify=False,
    ) as client:
        try:
            if not Path(file_path).exists():
                logger.error("File not found at path: %s", file_path)
                return {}

            content_type = (
                "video/mp4"
                if Path(file_path).suffix.lower() == ".mp4"
                else "video/MP2T"
            )

            with open(file_path, "rb") as f:
                files = {"file": (file_name, f, content_type)}
                data = {
                    "stream_id": stream_id,
                    "stream_name": stream_name,
                    "timestamp": timestamp,
                }

                response = await client.post(
                    f"{uri}/uploads-from-pi",
                    files=files,
                    data=data,
                )
                response.raise_for_status()
                result = response.json()
                logger.info(
                    "✅ Uploaded successfully: %s. Response: %s",
                    file_name,
                    result,
                )
                return result
        except httpx.ConnectError as e:
            logger.error("Upload connection error to %s: %s", uri, e)
            return {}
        except httpx.HTTPStatusError as e:
            logger.error(
                "Upload HTTP error %s: %s",
                e.response.status_code,
                e.response.text,
            )
            return {}
        except Exception:
            logger.error(
                "Unexpected error occurred during upload for %s.",
                file_name,
                exc_info=True,
            )
            return {}


async def trigger_ingestion(
    stream_id: str,
    stream_name: str,
    timestamp: str,
    audiovisual_api_uri: Optional[str] = None,
    ingestion_bucket: Optional[str] = None,
    delay_seconds: Optional[float] = None,
) -> None:
    """Notify the audiovisual API that a video is ready for analysis."""
    try:
        dt = datetime.fromisoformat(timestamp)
    except ValueError:
        logger.error("Invalid timestamp provided for ingestion: %s", timestamp)
        return

    api_uri = get_audiovisual_api_uri(audiovisual_api_uri)
    if not api_uri:
        logger.error("AUDIOVISUAL_API_URI is required for ingestion")
        return

    bucket = get_ingestion_bucket(ingestion_bucket)
    delay = get_ingestion_delay_seconds(delay_seconds)

    recording_date = dt.strftime("%Y-%m-%d")
    datetime_str = dt.strftime("%Y%m%d_%H%M")
    blob = f"tv/{stream_name}/{recording_date}/{stream_name}_{datetime_str}.mp4"

    payload = {
        "stream_id": stream_id,
        "stream_name": stream_name,
        "bucket": bucket,
        "blob": blob,
        "media_type": "video",
        "timestamp_str": timestamp,
    }

    if delay > 0:
        await asyncio.sleep(delay)

    async with httpx.AsyncClient(timeout=30, verify=False) as client:
        try:
            response = await client.post(
                f"{api_uri}/ingestion",
                json=payload,
            )
            response.raise_for_status()
            logger.info(
                "Triggered ingestion for %s (blob: %s)",
                stream_name,
                blob,
            )
        except httpx.HTTPError as exc:
            logger.error(
                "Failed to trigger ingestion for %s: %s",
                stream_name,
                exc,
            )
        except Exception:
            logger.error(
                "Unexpected error while triggering ingestion for %s",
                stream_name,
                exc_info=True,
            )
