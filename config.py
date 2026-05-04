import os
from pathlib import Path

# Base Directories
BASE_DIR = Path(os.environ.get("BASE_DIR", os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = BASE_DIR / "DATA"

# Subdirectories
SIRET_BATCHES_DIR = BASE_DIR / "data_api" / "siret_batches"
API_RESULTS_DIR = BASE_DIR / "data_api"

# File Paths
INPUT_PARQUET_PATH = DATA_DIR / "prospecting_leads_2026.parquet"

# File Prefixes
BATCH_FILE_PREFIX = "siret_batch"

# Scraping Configuration
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "5"))
MAX_BATCHES_PER_RUN = int(os.environ.get("MAX_BATCHES_PER_RUN", "10"))
REQUEST_DELAY = 0.75  # Seconds between requests per thread (Conservative for 7 req/s)
CHUNK_SIZE = 200  # Number of SIRETs to process before updating checkpoint

# Execution Time Limit (5.5 hours to allow Git push)
MAX_RUN_DURATION = 5.5 * 3600

# API Configuration
API_URL = "https://recherche-entreprises.api.gouv.fr/search"
USER_AGENT = "Mozilla/5.0 (DataMiningProject; contact@example.com)"

# Ensure directories exist
SIRET_BATCHES_DIR.mkdir(parents=True, exist_ok=True)
API_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
