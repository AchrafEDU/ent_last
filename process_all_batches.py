import json
import logging
import os
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

# Suppress common requests/urllib3 dependency warnings
from requests.exceptions import RequestsDependencyWarning
from urllib3.util.retry import Retry

from config import (
    API_RESULTS_DIR,
    API_URL,
    CHUNK_SIZE,
    MAX_BATCHES_PER_RUN,
    MAX_RUN_DURATION,
    MAX_WORKERS,
    REQUEST_DELAY,
    SIRET_BATCHES_DIR,
    USER_AGENT,
)

warnings.filterwarnings("ignore", category=RequestsDependencyWarning)

# --- CONFIGURATION ---

START_TIME = time.time()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger("batch_processor")

# Global state for rate limiting across threads
cooldown_until = 0.0
cooldown_lock = threading.Lock()


def setup_session() -> requests.Session:
    """
    Sets up a requests session with a retry strategy for common transient errors.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=10,
        backoff_factor=3,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        connect=5,
        read=5,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_siret_data(session: requests.Session, siret: str) -> Dict[str, Any]:
    """
    Fetches data for a single SIRET with manual rate limit handling and global cooldown check.
    """
    global cooldown_until
    params = {"q": siret}

    while True:
        # Respect global cooldown
        current_time = time.time()
        if current_time < cooldown_until:
            wait_needed = cooldown_until - current_time + 0.1
            time.sleep(wait_needed)
            continue

        try:
            response = session.get(API_URL, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_time = (
                    int(retry_after) if retry_after and retry_after.isdigit() else 20
                )
                logger.warning(
                    f"Rate limited for SIRET {siret}. Global cooldown for {wait_time}s"
                )
                with cooldown_lock:
                    cooldown_until = time.time() + wait_time
                continue

            elif response.status_code == 404:
                return {"error": "not_found", "status": 404}

            else:
                logger.error(f"Error {response.status_code} for SIRET {siret}")
                return {"error": "api_error", "status": response.status_code}

        except Exception as e:
            logger.error(f"Request exception for SIRET {siret}: {e}")
            time.sleep(2)
            return {"error": "exception", "details": str(e)}


def process_batch(
    batch_file: str, output_parquet: Path, session: requests.Session
) -> str:
    """
    Processes a single batch file and saves it as a parquet using multi-threading.
    """
    batch_base = os.path.basename(batch_file)
    logger.info(f"Processing batch: {batch_base}")

    checkpoint_path = output_parquet.with_suffix(".parquet.checkpoint")
    all_results: List[Dict[str, Any]] = []
    start_index = 0

    # Resume logic
    if output_parquet.exists() and checkpoint_path.exists():
        try:
            df_existing = pd.read_parquet(output_parquet)
            all_results = df_existing.to_dict("records")

            with open(checkpoint_path, "r", encoding="utf-8") as f:
                checkpoint_data = json.load(f)
                start_index = checkpoint_data.get("last_index", 0)

            logger.info(
                f"Resuming at index {start_index} with {len(all_results)} existing results..."
            )
        except Exception as e:
            logger.warning(f"Could not load resume state: {e}. Starting fresh.")
            all_results = []
            start_index = 0

    with open(batch_file, "r", encoding="utf-8") as f:
        sirets = [line.strip() for line in f.readlines()]

    total_sirets = len(sirets)

    def worker(siret: str) -> Dict[str, Any]:
        data = fetch_siret_data(session, siret)
        time.sleep(REQUEST_DELAY)
        if data and "results" in data and len(data["results"]) > 0:
            res = data["results"][0]
            res["queried_siret"] = siret
            res["api_status"] = "success"
            return res
        return {"queried_siret": siret, "api_status": "no_data"}

    start_time_batch = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(start_index, total_sirets, CHUNK_SIZE):
            chunk = sirets[i : i + CHUNK_SIZE]

            # Process chunk
            results = list(executor.map(worker, chunk))
            all_results.extend(results)

            # Save PROGRESS
            try:
                # Save data to Parquet first so the checkpoint never points past
                # data that was not actually written.
                df = pd.DataFrame(all_results)
                # Optimize complex type serialization
                cols_to_fix = [
                    col
                    for col in df.columns
                    if any(isinstance(x, (dict, list)) for x in df[col])
                ]

                for col in cols_to_fix:
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    )

                df.to_parquet(output_parquet, index=False)

                # Only advance the checkpoint after the Parquet file is safely saved.
                with open(checkpoint_path, "w", encoding="utf-8") as f:
                    json.dump({"last_index": i + len(chunk)}, f)

            except Exception as e:
                logger.error(f"Failed checkpoint/save: {e}")

            elapsed_batch = time.time() - start_time_batch
            processed = i + len(chunk)
            avg_speed = processed / (elapsed_batch + 0.001)

            # Calculate ETA
            remaining = total_sirets - processed
            eta_seconds = remaining / avg_speed if avg_speed > 0 else 0
            eta_minutes = eta_seconds / 60

            logger.info(
                f"[{processed}/{total_sirets}] Speed: {avg_speed:.2f} req/s | ETA: {eta_minutes:.1f}m"
            )

            # Check for global timeout
            if time.time() - START_TIME > MAX_RUN_DURATION:
                logger.warning(
                    "Approaching execution time limit. Results already saved."
                )
                return "TIMEOUT"

    # Final cleanup
    logger.info(f"Finished batch {batch_base}. Saved {len(all_results)} results.")
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    return "SUCCESS"


def main() -> None:
    """
    Main entry point for batch processing.
    """
    batch_files = sorted(list(SIRET_BATCHES_DIR.glob("siret_batch_*.txt")))

    if not batch_files:
        logger.error(f"No batches found in {SIRET_BATCHES_DIR}")
        return

    session = setup_session()
    batches_processed = 0

    for batch_file in batch_files:
        if batches_processed >= MAX_BATCHES_PER_RUN:
            logger.info(
                f"Reached MAX_BATCHES_PER_RUN ({MAX_BATCHES_PER_RUN}). Stopping."
            )
            break

        batch_name = batch_file.stem + ".parquet"
        output_path = API_RESULTS_DIR / batch_name
        checkpoint_path = output_path.with_suffix(".parquet.checkpoint")

        # Check if batch is already fully completed
        if output_path.exists() and not checkpoint_path.exists():
            logger.info(f"Skipping {batch_name} (already finished)")
            continue

        try:
            status = process_batch(str(batch_file), output_path, session)
            if status == "TIMEOUT":
                logger.info("Stopping run due to timeout.")
                break
            batches_processed += 1
        except Exception as e:
            logger.error(f"Error in batch {batch_name}: {e}")


if __name__ == "__main__":
    main()
