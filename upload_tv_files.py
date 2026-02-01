import asyncio
import httpx
import json
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime
from difflib import get_close_matches
from pathlib import Path

from upload_client import upload_recording, trigger_ingestion

# --- 1. SETUP LOGGING ---
LOG_FILE = Path(__file__).stem + ".log"
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_VALUE = logging._nameToLevel.get(LOG_LEVEL, logging.INFO)
logging.basicConfig(
    level=LOG_LEVEL_VALUE,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)


def log_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.error("Uncaught exception:", exc_info=(
        exc_type, exc_value, exc_traceback))


sys.excepthook = log_exception
logger = logging.getLogger(__name__)

# --- 2. CONFIGURATION ---

# Environment Variables (Relies solely on OS environment)
try:
    # VARIABLES for local file access and API interaction
    LOCAL_RECORDING_DIR = os.environ.get("LOCAL_RECORDING_DIR")
    GRAPHQL_URI = os.environ.get('GRAPHQL_URI')
    GRAPHQL_API_KEY = os.environ.get('GRAPHQL_API_KEY')
    MEDIASERVER_URI = os.environ.get("MEDIASERVER_URI")
    AUDIOVISUAL_API_URI = os.environ.get("AUDIOVISUAL_API_URI")
    # Optional strip() for robustness against invisible characters
    if LOCAL_RECORDING_DIR:
        LOCAL_RECORDING_DIR = str(
            Path(LOCAL_RECORDING_DIR.strip()).expanduser()
        )

    required_vars = {
        "GRAPHQL_URI": GRAPHQL_URI,
        "GRAPHQL_API_KEY": GRAPHQL_API_KEY,
        "MEDIASERVER_URI": MEDIASERVER_URI,
        "AUDIOVISUAL_API_URI": AUDIOVISUAL_API_URI,
    }

    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        missing_str = ", ".join(missing_vars)
        raise ValueError(
            "One or more essential environment variables are missing: "
            f"{missing_str}"
        )

    if LOCAL_RECORDING_DIR and not Path(LOCAL_RECORDING_DIR).is_dir():
        raise FileNotFoundError(
            "LOCAL_RECORDING_DIR not found or is not a directory: "
            f"{LOCAL_RECORDING_DIR}")

except Exception as e:
    logger.critical(f"Configuration Error: {e}")
    sys.exit(1)


CONVERSION_SUCCESS = []
CONVERSION_FAILURE = []
UPLOAD_SUCCESS = []
UPLOAD_FAILURE = []


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


SKIP_TS_CONVERSION = _env_flag("SKIP_TS_CONVERSION", default=False)
SKIP_INGESTION = _env_flag("SKIP_INGESTION", default=False)
RECURSIVE_SCAN = _env_flag("RECURSIVE_SCAN", default=True)
WAIT_FOR_COMPLETION = _env_flag("WAIT_FOR_COMPLETION", default=True)

COMPLETION_POLL_SECONDS = float(
    os.environ.get("COMPLETION_POLL_SECONDS", "30"))
COMPLETION_TIMEOUT_SECONDS = float(
    os.environ.get("COMPLETION_TIMEOUT_SECONDS", "7200"))

ELASTIC_NODE = os.environ.get(
    "ELASTIC_NODE") or os.environ.get("ELASTIC_CLOUD_URL")
ELASTIC_API_KEY = os.environ.get("ELASTIC_API_KEY")
ELASTIC_INDEX_PATTERN = os.environ.get("ELASTIC_INDEX_PATTERN", "tv_*")

MANIFEST_PATH = os.environ.get("MANIFEST_PATH")

ALLOWED_EXTENSIONS = {".ts", ".mp4"}
TARGET_DIRECTORIES = os.environ.get(
    "TARGET_DIRECTORIES") or os.environ.get("TARGET_DIRS")
BASE_RECORDING_DIR = os.environ.get("BASE_RECORDING_DIR")
DAILY_RECORDING_DATE = os.environ.get("DAILY_RECORDING_DATE")
DAILY_RECORDING_DATE_FORMAT = os.environ.get(
    "DAILY_RECORDING_DATE_FORMAT", "%Y-%m-%d")


def _parse_target_dirs(root_dir: Path | None) -> list[Path]:
    if not TARGET_DIRECTORIES:
        return []

    dirs: list[Path] = []
    for raw in TARGET_DIRECTORIES.split(","):
        candidate = raw.strip()
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if not path.is_absolute():
            if root_dir is None:
                logger.error(
                    "Relative TARGET_DIRECTORIES entry requires LOCAL_RECORDING_DIR: %s",
                    candidate,
                )
                continue
            path = root_dir / path
        if not path.exists() or not path.is_dir():
            logger.warning("Skipping invalid target dir: %s", path)
            continue
        dirs.append(path)

    return dirs


def _get_daily_recording_dir() -> Path | None:
    if not BASE_RECORDING_DIR:
        return None

    base_dir = Path(BASE_RECORDING_DIR).expanduser()
    if not base_dir.exists() or not base_dir.is_dir():
        logger.error(
            "BASE_RECORDING_DIR not found or is not a directory: %s", base_dir)
        return None

    if DAILY_RECORDING_DATE:
        date_part = DAILY_RECORDING_DATE.strip()
    else:
        date_part = datetime.now().strftime(DAILY_RECORDING_DATE_FORMAT)

    return base_dir / date_part


def _load_manifest(path: str) -> dict:
    try:
        manifest_path = Path(path)
        if not manifest_path.exists():
            return {}
        with manifest_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        logger.warning("Failed to read manifest %s", path, exc_info=True)
        return {}


def _save_manifest(path: str, manifest: dict) -> None:
    try:
        manifest_path = Path(path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
    except Exception:
        logger.warning("Failed to write manifest %s", path, exc_info=True)


def _update_manifest(manifest: dict, file_path: Path, status: str, reason: str | None = None) -> None:
    manifest[str(file_path)] = {
        "status": status,
        "reason": reason,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _iter_recording_files(root_dir: Path) -> list[Path]:
    if RECURSIVE_SCAN:
        files = [
            path
            for path in root_dir.rglob("*")
            if path.is_file() and path.suffix.lower() in ALLOWED_EXTENSIONS
        ]
    else:
        files = [
            path
            for path in root_dir.glob("*")
            if path.is_file() and path.suffix.lower() in ALLOWED_EXTENSIONS
        ]
    return sorted(files)


def _list_recording_dirs(files: list[Path]) -> list[str]:
    dirs = sorted({str(path.parent) for path in files})
    return dirs


async def _wait_for_es_completion(stream_id: str, timestamp: str) -> bool:
    if not WAIT_FOR_COMPLETION:
        return True
    if not ELASTIC_NODE or not ELASTIC_API_KEY:
        logger.warning(
            "Skipping ES completion check (missing ELASTIC_NODE or ELASTIC_API_KEY)")
        return True

    deadline = time.monotonic() + COMPLETION_TIMEOUT_SECONDS
    headers = {
        "Authorization": f"ApiKey {ELASTIC_API_KEY}",
        "Content-Type": "application/json",
    }

    query = {
        "size": 1,
        "sort": [{"timestamp": {"order": "desc"}}],
        "query": {
            "bool": {
                "filter": [
                    {"term": {"status.complete": True}},
                    {"term": {"timestamp": timestamp}},
                ],
                "should": [
                    {"term": {"stream_id": stream_id}},
                    {"term": {"stream_id.keyword": stream_id}},
                ],
                "minimum_should_match": 1,
            }
        },
    }

    search_url = f"{ELASTIC_NODE.rstrip('/')}/{ELASTIC_INDEX_PATTERN}/_search"
    async with httpx.AsyncClient(timeout=30, verify=False) as client:
        while time.monotonic() < deadline:
            try:
                response = await client.post(search_url, headers=headers, json=query)
                response.raise_for_status()
                payload = response.json()
                hits = payload.get("hits", {}).get("hits", [])
                if hits:
                    return True
            except Exception:
                logger.warning("ES completion check failed", exc_info=True)

            await asyncio.sleep(COMPLETION_POLL_SECONDS)

    return False


def record_conversion_result(file_name: str, success: bool,
                             reason: str | None = None) -> None:
    """Track conversion outcomes for summary logging."""
    if success:
        CONVERSION_SUCCESS.append(file_name)
        logger.debug("Recorded conversion success for %s", file_name)
    else:
        CONVERSION_FAILURE.append((file_name, reason or "Unknown reason"))
        logger.debug("Recorded conversion failure for %s: %s",
                     file_name, reason)


def record_upload_result(file_name: str, success: bool,
                         reason: str | None = None) -> None:
    """Track upload outcomes for summary logging."""
    if success:
        UPLOAD_SUCCESS.append(file_name)
        logger.debug("Recorded upload success for %s", file_name)
    else:
        UPLOAD_FAILURE.append((file_name, reason or "Unknown reason"))
        logger.debug("Recorded upload failure for %s: %s",
                     file_name, reason)


# --- 3. ASYNC FUNCTIONS ---

async def check_file_streams(input_path: str) -> dict:
    """Checks what streams are available in the input file using ffprobe."""
    command = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        str(input_path)
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            import json

            probe_data = json.loads(stdout.decode())
            streams = probe_data.get("streams", [])

            video_streams = []
            audio_streams = []
            data_streams = []
            subtitle_streams = []

            for stream in streams:
                stream_type = stream.get("codec_type")
                if stream_type == "video":
                    video_streams.append(stream)
                elif stream_type == "audio":
                    audio_streams.append(stream)
                elif stream_type == "data":
                    data_streams.append(stream)
                elif stream_type == "subtitle":
                    subtitle_streams.append(stream)

            has_data_only = (
                len(data_streams) > 0 and
                len(video_streams) == 0 and
                len(audio_streams) == 0
            )

            return {
                "has_video": bool(video_streams),
                "has_audio": bool(audio_streams),
                "has_data_only": has_data_only,
                "total_streams": len(streams),
                "video_count": len(video_streams),
                "audio_count": len(audio_streams),
                "data_count": len(data_streams),
                "video_indexes": [s.get("index") for s in video_streams],
                "audio_indexes": [s.get("index") for s in audio_streams],
                "subtitle_indexes": [
                    s.get("index") for s in subtitle_streams
                ],
            }
        else:
            logger.error(f"ffprobe failed for {input_path}. "
                         f"Code: {process.returncode}")
            return {"error": True}

    except FileNotFoundError:
        logger.error("ffprobe not found. Please ensure FFmpeg is installed.")
        return {"error": True}
    except Exception:
        logger.error(f"Error during ffprobe execution for {input_path}.",
                     exc_info=True)
        return {"error": True}


async def convert_to_mp4(input_path: str) -> str | None:
    """Convert a TS file to MP4, returning the MP4 path or None on failure."""
    original_path = Path(input_path)

    if SKIP_TS_CONVERSION and original_path.suffix.lower() == ".ts":
        logger.info(
            "Skipping conversion for %s due to SKIP_TS_CONVERSION flag.",
            original_path.name,
        )
        record_conversion_result(
            original_path.name,
            True,
            "Conversion skipped via flag",
        )
        return str(original_path)

    if original_path.suffix.lower() != ".ts":
        logger.info(
            "Skipping conversion for non-TS file: %s",
            original_path.name,
        )
        record_conversion_result(
            original_path.name,
            True,
            "Input file is not TS",
        )
        return str(original_path)

    logger.info("Analyzing streams in %s...", original_path.name)
    stream_info = await check_file_streams(input_path)

    if stream_info.get("error"):
        logger.warning(
            "Stream analysis failed for %s; skipping upload.",
            original_path.name,
        )
        record_conversion_result(
            original_path.name,
            False,
            "Stream analysis failed",
        )
        return None

    logger.info(
        "Stream analysis for %s: video=%s audio=%s data=%s",
        original_path.name,
        stream_info.get("video_count", 0),
        stream_info.get("audio_count", 0),
        stream_info.get("data_count", 0),
    )

    if stream_info.get("has_data_only"):
        logger.warning(
            "File %s contains only data streams; skipping upload.",
            original_path.name,
        )
        record_conversion_result(
            original_path.name,
            False,
            "Data-only transport stream",
        )
        return None

    if not stream_info.get("has_video") and not stream_info.get("has_audio"):
        logger.warning(
            "No video or audio streams in %s; skipping upload.",
            original_path.name,
        )
        record_conversion_result(
            original_path.name,
            False,
            "No video/audio streams detected",
        )
        return None

    temp_dir = Path(tempfile.mkdtemp(prefix="tv_upload_"))
    output_path = temp_dir / f"{original_path.stem}.mp4"
    logger.info(
        "Starting conversion of %s to %s...",
        original_path.name,
        output_path.name,
    )

    video_indexes = stream_info.get("video_indexes") or []
    audio_indexes = stream_info.get("audio_indexes") or []

    selected_video = video_indexes[0] if video_indexes else None
    selected_audio = audio_indexes[0] if audio_indexes else None

    logger.debug(
        "Selected stream indexes -> video: %s audio: %s",
        selected_video,
        selected_audio,
    )

    # Use larger analyze/probe values so FFmpeg can discover tricky streams.
    command = [
        "ffmpeg",
        "-analyzeduration",
        "10M",
        "-probesize",
        "10M",
        "-i",
        str(original_path),
        "-ignore_unknown",
    ]

    if selected_video is not None:
        command.extend(
            [
                "-map",
                f"0:{selected_video}",
                "-c:v",
                "libx264",
                "-preset",
                "medium",
                "-crf",
                "22",
            ]
        )
    else:
        command.append("-vn")

    if selected_audio is not None:
        command.extend(
            [
                "-map",
                f"0:{selected_audio}",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
            ]
        )
    else:
        command.append("-an")

    command.extend(
        [
            "-movflags",
            "+faststart",
            "-y",
            str(output_path),
        ]
    )

    def cleanup_temp_file() -> None:
        if output_path.exists():
            output_path.unlink()
        parent_dir = output_path.parent
        if parent_dir.name.startswith("tv_upload_"):
            try:
                parent_dir.rmdir()
            except OSError:
                pass

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("✅ Conversion successful: %s", output_path.name)
            record_conversion_result(
                original_path.name,
                True,
                "Converted to MP4",
            )
            return str(output_path)

        logger.error(
            "FFmpeg conversion failed for %s with code %s.",
            original_path.name,
            process.returncode,
        )
        logger.error("FFmpeg stderr:%s%s", os.linesep, stderr.decode())
        cleanup_temp_file()
        record_conversion_result(
            original_path.name,
            False,
            f"FFmpeg exit code {process.returncode}",
        )
        return None

    except FileNotFoundError:
        logger.error("FFmpeg not found. Please ensure FFmpeg is installed.")
        cleanup_temp_file()
        record_conversion_result(
            original_path.name,
            False,
            "FFmpeg executable not found",
        )
        return None
    except Exception:
        logger.error(
            "Error during FFmpeg execution for %s.",
            original_path.name,
            exc_info=True,
        )
        cleanup_temp_file()
        record_conversion_result(
            original_path.name,
            False,
            "Unexpected FFmpeg error",
        )
        return None


async def get_tv_streams() -> list:
    """Fetches list of TV streams from the GraphQL API."""
    logger.info("Fetching TV streams from GraphQL API...")
    query = """
    query ($filter: FindTvStreamsInput!){
        findTvStreams(filter: $filter){
            id
            tv_stream_name
            streaming_link
            format
        }
    }
    """
    variables = {"filter": {}}
    headers = {
        "Content-Type": "application/json",
        "x-api-key": GRAPHQL_API_KEY
    }

    try:
        async with httpx.AsyncClient(timeout=60, verify=False) as client:
            r = await client.post(
                GRAPHQL_URI,
                json={"query": query, "variables": variables},
                headers=headers
            )
            r.raise_for_status()
            data = r.json()
            streams = data.get("data", {}).get("findTvStreams", [])
            logger.info(f"Successfully retrieved {len(streams)} TV streams.")
            return streams
    except Exception:
        logger.error(
            "An unexpected error occurred while fetching TV streams.",
            exc_info=True)
        return []


def _normalize_stream_name(name: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", name.upper())


def find_stream_match(streams: list, tv_station_name: str, filename: str):
    """Find the best stream match using exact, contains, filename, then fuzzy."""
    stream_names = [stream["tv_stream_name"] for stream in streams]
    normalized_station = _normalize_stream_name(tv_station_name)
    normalized_filename = _normalize_stream_name(filename)

    exact_match = next(
        (name for name in stream_names
         if _normalize_stream_name(name) == normalized_station),
        None,
    )

    contains_match = None
    if not exact_match:
        for name in stream_names:
            normalized_name = _normalize_stream_name(name)
            if normalized_station and normalized_station in normalized_name:
                contains_match = name
                break

    best_match = []
    if not exact_match and not contains_match:
        filename_contains_match = next(
            (
                name
                for name in stream_names
                if _normalize_stream_name(name)
                and _normalize_stream_name(name) in normalized_filename
            ),
            None,
        )

        if filename_contains_match:
            best_match = [filename_contains_match]
        else:
            best_match = get_close_matches(
                tv_station_name,
                stream_names,
                n=1,
                cutoff=0.85,
            )

    matched_name = exact_match or contains_match or (
        best_match[0] if best_match else None)

    if not matched_name:
        return None

    return next(
        stream
        for stream in streams
        if stream["tv_stream_name"] == matched_name
    )


async def process_and_upload(streams: list, recording_path: str) -> dict:
    """Process a single recording: convert when possible, match, upload."""
    original_path = Path(recording_path)
    current_path = recording_path

    result = {
        "file": str(original_path),
        "status": "skipped",
        "reason": None,
    }

    logger.info(f"--- Starting processing for file: {original_path.name} ---")

    try:
        # Step 0: Extract relevant details from filename for matching logic
        filename = original_path.stem
        pattern = (
            r"[-_]"
            r"(?:Signet\s+(?P<station_signet>[A-Za-z0-9\s]+)"
            r"|PANG-(?P<station_pang>[A-Za-z0-9\s]+))"
            r"[-_]"
            r"(?P<date>\d{4}-\d{2}-\d{2})"
            r"[-_](?P<time>\d{2}[-_]\d{2})[-_]?"
        )
        match = re.search(pattern, filename)

        if not match:
            logger.warning(
                "Skipping file %s: invalid filename format.",
                filename,
            )
            result["reason"] = "invalid filename format"
            return result

        # Extract details
        tv_station_name = (
            match.group("station_signet")
            or match.group("station_pang")
            or ""
        ).replace("Signet", "").strip()
        date = match.group("date")
        timestamp = match.group("time").replace("-", ":")
        full_timestamp = f"{date}T{timestamp}:00"

        # Step 1: Match with GraphQL (before any conversion)
        matched_stream = find_stream_match(streams, tv_station_name, filename)

        if not matched_stream:
            logger.warning(
                "Skipping file %s: no stream match for %s.",
                filename,
                tv_station_name,
            )
            result["reason"] = f"no stream match for {tv_station_name}"
            return result
        stream_id = matched_stream["id"]
        stream_name = matched_stream["tv_stream_name"]
        logger.info(
            "Match found: %s -> %s",
            tv_station_name,
            stream_name,
        )

        # Step 2: Conversion (Only if it's a .ts file)
        if original_path.suffix.lower() == '.ts':
            converted_path = await convert_to_mp4(recording_path)
            if not converted_path:
                logger.error(
                    "Skipping upload for %s: conversion failed or "
                    "no media streams.",
                    original_path.name,
                )
                result["status"] = "failed"
                result["reason"] = "conversion failed"
                return result

            current_path = converted_path
            logger.info(
                "Using converted file for upload: %s",
                Path(current_path).name,
            )

        # Step 3: Upload the converted file (current_path)
        upload_result = await upload_recording(
            file_path=current_path,
            stream_id=stream_id,
            stream_name=stream_name,
            timestamp=full_timestamp
        )

        # Step 4: Cleanup (Only removes the temporary .mp4 file)
        if upload_result and upload_result.get("success"):
            if SKIP_INGESTION:
                logger.info(
                    "Skipping ingestion for %s due to SKIP_INGESTION flag.",
                    stream_name,
                )
                result["status"] = "uploaded"
                result["reason"] = "ingestion skipped"
            else:
                await trigger_ingestion(stream_id, stream_name, full_timestamp)
                result["status"] = "uploaded"
            record_upload_result(
                original_path.name,
                True,
                "Uploaded successfully",
            )
            logger.info("Starting file cleanup...")

            # Remove the temporary MP4 generated during conversion.
            if current_path != recording_path and Path(current_path).exists():
                Path(current_path).unlink()
                logger.info(
                    "Deleted temporary uploaded file: %s",
                    Path(current_path).name,
                )

                temp_dir = Path(current_path).parent
                if temp_dir.name.startswith("tv_upload_"):
                    try:
                        temp_dir.rmdir()
                        logger.debug(
                            "Removed temporary directory %s",
                            temp_dir,
                        )
                    except OSError:
                        logger.debug(
                            "Temporary directory %s not removed (not empty).",
                            temp_dir,
                        )

            logger.info(
                "Original file %s was NOT deleted.",
                original_path.name,
            )

            logger.info("✅ Upload and cleanup complete.")

            if not SKIP_INGESTION:
                completed = await _wait_for_es_completion(stream_id, full_timestamp)
                if completed:
                    result["status"] = "completed"
                else:
                    result["status"] = "timeout"
                    result["reason"] = "ES completion timeout"
        else:
            failure_reason = None
            if upload_result:
                failure_reason = (
                    upload_result.get("message")
                    or upload_result.get("error")
                    or str(upload_result)
                )
            else:
                failure_reason = "Upload response empty"
            record_upload_result(
                original_path.name,
                False,
                failure_reason,
            )
            logger.error(
                "⚠️ Upload failed for %s. All files remain for manual retry.",
                original_path.name,
            )
            result["status"] = "failed"
            result["reason"] = failure_reason
    except Exception:
        logger.critical(
            "Critical failure while processing file: %s.",
            original_path.name,
            exc_info=True,
        )
        result["status"] = "failed"
        result["reason"] = "unexpected exception"

    logger.info("--- Finished processing for file: %s ---", original_path.name)

    return result


async def upload_single_recording(recording_name: str) -> None:
    """Upload a specific recording located inside LOCAL_RECORDING_DIR."""
    base_dir = Path(LOCAL_RECORDING_DIR)
    target_path = Path(recording_name)

    if not target_path.is_absolute():
        target_path = base_dir / target_path

    if not target_path.exists():
        logger.error(
            "Target recording '%s' not found inside %s",
            recording_name,
            base_dir,
        )
        return

    streams = await get_tv_streams()
    if not streams:
        logger.error("Could not retrieve TV streams. Aborting single upload.")
        return

    await process_and_upload(streams, str(target_path))


def upload_single_recording_sync(recording_name: str) -> None:
    """Synchronous helper for testing single recording uploads."""
    asyncio.run(upload_single_recording(recording_name))


async def main():
    """Fetch TV streams and process local recordings."""
    logger.info("--- Script Start: Processing Local Directory ---")

    streams = await get_tv_streams()

    if not streams:
        logger.error("Could not retrieve TV streams. Exiting script.")
        return

    root_dir = Path(LOCAL_RECORDING_DIR) if LOCAL_RECORDING_DIR else None
    target_dirs = _parse_target_dirs(root_dir)

    if not target_dirs:
        daily_dir = _get_daily_recording_dir()
        if daily_dir:
            target_dirs = [daily_dir]

    if not target_dirs:
        logger.error(
            "No valid TARGET_DIRECTORIES or daily recording dir found. Nothing to process."
        )
        return

    recording_files: list[Path] = []
    for target_dir in target_dirs:
        recording_files.extend(_iter_recording_files(target_dir))

    recording_files = sorted({path.resolve() for path in recording_files})
    recording_dirs = _list_recording_dirs(recording_files)

    if recording_dirs:
        logger.info("Recording directories discovered (%d):",
                    len(recording_dirs))
        for directory in recording_dirs:
            logger.info("- %s", directory)

    if not recording_files:
        logger.info("No .ts/.mp4 files found in target directories.")
        return

    if not MANIFEST_PATH:
        manifest_base = target_dirs[0]
        manifest_path = manifest_base / ".upload_manifest.json"
    else:
        manifest_path = Path(MANIFEST_PATH).expanduser()

    manifest = _load_manifest(str(manifest_path))

    logger.info("Starting to process %d recording file(s)...",
                len(recording_files))
    for file_path in recording_files:
        manifest_entry = manifest.get(str(file_path))
        if manifest_entry and manifest_entry.get("status") in {"completed", "uploaded"}:
            logger.info("Skipping already processed file: %s", file_path.name)
            continue

        result = await process_and_upload(streams, str(file_path))
        _update_manifest(
            manifest,
            file_path,
            result.get("status", "unknown"),
            result.get("reason"),
        )
        _save_manifest(str(manifest_path), manifest)

    if CONVERSION_SUCCESS:
        logger.info(
            "Conversion success (%d): %s",
            len(CONVERSION_SUCCESS),
            ", ".join(CONVERSION_SUCCESS),
        )
    if CONVERSION_FAILURE:
        failure_details = "; ".join(
            f"{name} [{reason}]" for name, reason in CONVERSION_FAILURE
        )
        logger.warning(
            "Conversion failed (%d): %s",
            len(CONVERSION_FAILURE),
            failure_details,
        )
    if UPLOAD_SUCCESS:
        logger.info(
            "Upload success (%d): %s",
            len(UPLOAD_SUCCESS),
            ", ".join(UPLOAD_SUCCESS),
        )
    if UPLOAD_FAILURE:
        upload_failures = "; ".join(
            f"{name} [{reason}]" for name, reason in UPLOAD_FAILURE
        )
        logger.warning(
            "Upload failed (%d): %s",
            len(UPLOAD_FAILURE),
            upload_failures,
        )

    logger.info("--- Script End: All file uploads completed. ---")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Script manually interrupted by user (CtrlC).")
    except Exception:
        logger.critical(
            "An unexpected error occurred in the main execution block.",
            exc_info=True)
        sys.exit(1)
