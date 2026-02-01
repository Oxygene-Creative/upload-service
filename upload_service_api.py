import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

from upload_client import upload_recording, trigger_ingestion

logger = logging.getLogger("upload_service_api")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_VALUE = logging._nameToLevel.get(LOG_LEVEL, logging.INFO)
logging.basicConfig(level=LOG_LEVEL_VALUE)

app = FastAPI(
    title="Upload Service",
    description="Upload service for forwarding media to the media server",
    version="1.0.0",
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


def _resolve_timestamp(value: Optional[str]) -> str:
    if value:
        return value
    return datetime.now().replace(microsecond=0).isoformat()


@app.post("/uploads")
async def upload_file(
    file: UploadFile = File(...),
    stream_id: str = Form(...),
    stream_name: str = Form(...),
    timestamp: Optional[str] = Form(None),
    source: Optional[str] = Form(None),
):
    filename = file.filename or ""
    if not (filename.endswith(".mp4") or filename.endswith(".ts")):
        raise HTTPException(
            status_code=400,
            detail="File must be a .ts or .mp4 file",
        )

    suffix = ".mp4" if filename.endswith(".mp4") else ".ts"
    timestamp_value = _resolve_timestamp(timestamp)

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        while True:
            chunk = await file.read(4 * 1024 * 1024)
            if not chunk:
                break
            temp_file.write(chunk)
        temp_path = temp_file.name

    logger.info(
        "Received upload from %s for stream %s: %s",
        source or "unknown",
        stream_name,
        filename,
    )

    upload_result = await upload_recording(
        file_path=temp_path,
        stream_id=stream_id,
        stream_name=stream_name,
        timestamp=timestamp_value,
    )

    if not upload_result or not upload_result.get("success"):
        Path(temp_path).unlink(missing_ok=True)
        return {
            "success": False,
            "message": "Upload to media server failed",
            "details": upload_result or {},
        }

    await trigger_ingestion(
        stream_id=stream_id,
        stream_name=stream_name,
        timestamp=timestamp_value,
    )

    Path(temp_path).unlink(missing_ok=True)

    return {
        "success": True,
        "message": "Uploaded and queued for processing",
        "media_server": upload_result,
    }
