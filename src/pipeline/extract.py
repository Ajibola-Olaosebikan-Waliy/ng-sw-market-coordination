"""
ABOUT extract.py:
----------
The marketfed automated data pipeline.

PURPOSE
-------
To extract WFP Nigeria food price data from the
Humanitarian Data Exchange (HDX) platform and saves
a filtered, timestamped raw CSV to data/raw/.

INPUT
-----
- HDX API endpoint (configured in config.py)
- No local files required — this is the pipeline entry point

EXPECTED OUTPUT
------
A timestamped CSV file at:
  data/raw/wfp_nga_tomato_onion_YYYYMMDD_HHMMSS.csv

Contains all columns in the WFP dataset, filtered to:
1. Country     : Nigeria (adm0_name)
2. Markets     : Ibadan, Lagos, Dawanau (mkt_name)
3. Commodities : Tomatoes - Retail, Onions - Retail (cmname)
NOTE: The data at the retail unit is used for modelling

PIPELINE POSITION
-----------------
extract.py → validate.py → transform.py → load.py

EXECUTION
---------
As part of Prefect flow : python -m pipeline.run
Standalone (no Prefect) : python -m pipeline.extract

NOTE 
----
1. Only the raw price columns is known 
2. Lag features, rolling
statistics, cross-market spreads, and inflation
adjustment are engineered in transform.py
"""

"""
Python Imports
--------------
About: 
1. requests:handles the actual network connection to HDX website, streams 
the response line by line rather than downloading everything before processing.
2. task: a decorator with retries, logging, and observability,and turns Python's 
function into a prefect task 
3. get_run_logger: it writes to the Prefect dashboard when running inside a flow, rather than just to the terminal
"""
import logging
from datetime import datetime, timezone
from io import StringIO #(enables pandas to read without touching the file system)
from pathlib import Path
import pandas as pd
import requests
from prefect import task, get_run_logger
from pipeline.config import (
    HDX_NIGERIA_URL,
    TARGET_COMMODITIES,
    TARGET_MARKETS,
    RAW_DIR,
    RAW_FILE_PREFIX,
    DOWNLOAD_TIMEOUT_SECONDS,
)

def _ensure_dir(path: Path) -> Path:
  """
  This function creates a directory at path if it does not exist.

  Parameter 'path : Path' is the directory path to be created.

  Returns the same path, so the file.csv can be attached to the _ensure_dir(RAW_DIR) directory

  """
  path.mkdir(parents=True, exist_ok=True)
  return path

def _timestamped_path(prefix: str, ext: str = "csv") -> Path:
  """
  1. This function builds a unique, timestamped output in the path: data/raw/.
  2. The Timestamps make every run uniquely identifiable 
  """
  time = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
  file_name = f"{prefix}_{time}.{ext}"
  return _ensure_dir(RAW_DIR) / file_name

def _stream_and_filter(logger) -> pd.DataFrame:
  """
  This function is designed to stream (not to store data on the system's disk) the global CSV 
  file from WFP-HDX data line by line and keeps only Nigerians rows, and filter those
  rows to the target markets and commodities in the config.py file

  """
  ### Connecton to the WFP HDX API ###
  logger.info("Connecting to HDX API...")
  logger.info(f"URL: {HDX_NIGERIA_URL[:70]}...")
  response = requests.get(
    HDX_NIGERIA_URL,
    stream=True,
    timeout=DOWNLOAD_TIMEOUT_SECONDS,

  )
  response.raise_for_status()

  ###___ Collect Nigeria-focused rows via line streaming__##
  header: str | None =None
  matched_lines: list[str] = []
  total_vetted_lines = 0

  for each_line in response.iter_lines():
    if not each_line:
      continue
    line = each_line.decode("utf-8")
    total_vetted_lines += 1

    ## save the header: the first line in the csv header row ##
    if header is None:
      header = line
      logger.info(f"CSV header saved: {header[:80]}...")
      continue

    if line.startswith("#"): ##Drop the HDX metadata tag line
      continue

    matched_lines.append(line)
  logger.info(f"Vetted {total_vetted_lines:,} total lines")
  logger.info(f"Nigeria rows fetched: {len(matched_lines):,}")
  if not matched_lines:
    raise ValueError(f"No rows found found after streaming.Check HDX_NIGERIA_URL in config.py")

  ###__Create a DataFrame for the collected rows__##
  csv_data = header + "\n" + "\n".join(matched_lines)
  data_frame = pd.read_csv(StringIO(csv_data))

  logger.info(f"DataFrame shape before filtering: {data_frame.shape}")

  ## dynamic column mapping scheme to prevent key errors if headers changes in future##
  #current_columns = [col.strip().lower for col in data_frame.columns]

  ## Address commodity variants ##
  #commodity_variants = ["cm_name", "commodity","cmname","item"]
  #cm_col = next((orig for orig, col in zip(data_frame.columns, current_columns) if col in commodity_variants),None)

  ## Address the market variant
  #market_variants = ["mkt_name", "market", "mktname", "location"]
  #mkt_col = next((orig for orig, col in zip(data_frame.columns, current_columns) if col in market_variants), None)

  #if not cm_col or not mkt_col:
    #raise KeyError(f"Could not map dataset schema dynamically. Columns found: {data_frame.columns.tolist()}")

  data_frame = data_frame[data_frame["commodity"].isin(TARGET_COMMODITIES)]

  logger.info(f"Rows after commodity filter '{TARGET_COMMODITIES}': {len(data_frame):,}")

  data_frame = data_frame[data_frame["market"].isin(TARGET_MARKETS)]
  
  logger.info(f"Rows after market filter '{TARGET_MARKETS}:' {len(data_frame):,}")

  return data_frame

@task(
  name="extract-wfp-nigeria",
  retries=3,
  retry_delay_seconds=15,
  description=(
    "Stream WFP Nigeria food prices from HDX."
    "Filter to target markets and commodities."
    "Save the timestamped raw CSV to data/raw/."
  ),
)
def extract_data() -> Path:
  """
  This function orchestrates the full extraction sequence:
  1. Stream and filter the HDX dataset
  2. Validate the result is not empty
  3. Save to a timestamped CSV in data/raw/
  4. Return the saved file path
  """

  try:
    logger = get_run_logger()
  except Exception:
    logger = logging.getLogger("src.pipeline.extract")

  logger.info("___Started data extraction___")

  data_frame = _stream_and_filter(logger)

  if data_frame.empty:
    raise ValueError(
      "The filtered DataFrame is empty"
    )
  ##__The saved output path__##
  output_path = _timestamped_path(RAW_FILE_PREFIX)
  data_frame.to_csv(output_path, index=False)
  logger.info(f"Saved {len(data_frame):,} rows to {output_path.name}")
  logger.info(f"Columns: {data_frame.columns.tolist()}")
  logger.info("Data extraction completed")

  return output_path


if __name__ == "__main__":
  logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
  )
  log = logging.getLogger(__name__)
  log.info("Running the extract.pyas a standalone script")
  saved_path = extract_data.fn()
  data_frame = pd.read_csv(saved_path)
  print(f"File saved : {saved_path}")
