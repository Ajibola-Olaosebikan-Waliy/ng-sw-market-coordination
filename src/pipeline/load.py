"""
About:
1. This file partitions the transformed data (in parquet file) into one Parquet file per federated learning client (market node)
2. Each partitioned file in saved into its own directory based on the city where the market is. E.g:
    * data/clients/ibadan/
    * data/clients/lagos/
    * data/clients/dawanu/
3. In this partitioning, the privacy boundary of true horizontal
   federated learning is simulated. That is, each client node loads only its own partition and never sees the
   data from other markets.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd 
from prefect import task, get_run_logger

from pipeline.config import(
    TARGET_MARKETS,
    CLIENT_PARTITION_PATHS,
    QUALITY_DIR,
)

####---- Internal Helpers ----- ####
def _create_dir(path:Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path  ### it creates directory at path if it does not exist

def _timestamped_filename(prefix: str, ext: str ="parquet") -> str:
    time_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{time_stamp}.{ext}"

def _partition_by_market(data_frame: pd.DataFrame, logger) -> dict[str, Path]:
    """
    This function splits the processed DataFrame into one Parquet file per
    market and save each file to its isolated client directory. The file is
    stored as a Parquet and not a CSV to preserve the data types and formats. 
    """
    partition_paths: dict[str, Path] = {}

    for market in TARGET_MARKETS:
        market_df = data_frame[
            data_frame["market"] == market
        ].copy().reset_index(drop=True)

        if market_df.empty:
            logger.warning(
                f"partition by market: no rows found"
                f"for market '{market}'"
            )
            continue
        ## Build output path for each client
        client_dir = _create_dir(CLIENT_PARTITION_PATHS[market])
        safe_name = market.lower().replace(" ", "_")
        filename = _timestamped_filename(f"wfp_{safe_name}_grains")
        output_path = client_dir / filename

        ## Save Each partition
        market_df.to_parquet(output_path, index=False)
        partition_paths[market] = output_path

        logger.info(
            f" {market:<10} -> {output_path.name}"
            f"({len(market_df):,} rows)"
        )
    return partition_paths

def _partitioned_data_summarized_report(partition_paths: dict[str, Path], logger,) -> dict:
    """
    This functions inspect each saved partition and build a structured summary report
    """
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_stage": "load.py",
        "total_clients": len(partition_paths),
        "clients": {},
    }
    for market, path in partition_paths.items():
        data_frame = pd.read_parquet(path)

        ## row count based on each commodity
        commodity_counts = (
            data_frame["commodity"].value_counts().to_dict()
        )
        ## Feature columns (engineered columns)
        engineered_cols = [
            c for c in data_frame.columns
            if any(x in c for x in ["lag", "roll", "spread", "per_kg"])
        ]

        ## Non-empty rates for engineered features
        feature_null_rates = {
            col: round(
                data_frame[col].isna().mean() * 100, 1
            )
            for col in engineered_cols
        }

        summary["clients"][market] = {
            "partition_file": path.name,
            "total_rows": len(data_frame),
            "total_columns": len(data_frame.columns),
            "date_range": {
                "earliest": str(data_frame["date"].min()),
                "latest": str(data_frame["date"].max()),
            },
            "commodities": commodity_counts,
            "engineered_features": len(engineered_cols),
            "feature_null_rates": feature_null_rates,
        }
        logger.info(
            f" {market:<10} summary: "
            f"{len(data_frame):,} rows "
            f"{len(engineered_cols)} features "
            f"Date Range "
            f"{data_frame['date'].min().date()}"
            f"{data_frame['date'].max().date()}"
        )
    return summary

@task(
    name="load-wfp-nigeria",
    retries=3,
    retry_delay_seconds=30,
    description=(
        "Partition processed WFP Nigeria grains data"
        "into one Parquet file per federated client. "
        "Save partition summary to data/quality_reports/."
    ),
)
def load_data(processed_path: Path) -> dict[str, Path]:
    """
    This function orchestrates the full loading sequence. It reads the processed
    Parquet file, calls the split and summary internal helpers to split, save, and 
    writes the summary in JSON. 
    """
    try:
        logger = get_run_logger()
    except Exception:
        logger = logging.getLogger(__name__)
    logger.info("=== Data Loading Started ===")
    logger.info(f"Input file: {processed_path}")

    ##-- Load processed Parquet file--##
    if not processed_path.exists():
        raise FileNotFoundError(
            f"Processed file not found: {processed_path}"
        )
    data_frame = pd.read_parquet(processed_path)
    logger.info(
        f"Loaded {len(data_frame):,} rows "
        f"{len(data_frame.columns)} columns"
    )

    ##-- Partition the Parquet file by market names --##
    logger.info("Partitioning by market names...")
    partition_paths = _partition_by_market(data_frame,logger)

    ##-- Build partition summary--##
    logger.info("Building partition summary...")
    summary = _partitioned_data_summarized_report(partition_paths, logger)

    ##-- Save summary in JSON --##
    from pipeline.transform import _ensure_dir
    time_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    summary_path = (_ensure_dir(QUALITY_DIR)/f"partition_summary_{time_stamp}.json")

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info(f"Partition summary saved to {summary_path.name}")

    ##-- Log final summary ---##
    logger.info("=== Partition Summary ===")
    for market, path in partition_paths.items():
        logger.info(f"{market:<10} -> {path.name}")
    logger.info(
        f"Total clients partitioned: "
        f"{len(partition_paths)}"
    )
    logger.info("=== Loaded Data Successfully")

    return partition_paths

## == Standalone Execution == ##
if __name__ == "__main__":
    import sys
    from pipeline.validate import validate_data
    from pipeline.transform import transform_data

    logging.basicConfig(
        level=logging.INFO,
        format="(asctime)s | %(levelname)s | %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger(__name__)

    ## --- Locate the most recent processed file ---##
    processed_files = sorted(
        Path("data/processed").glob("*.parquet")
    )
    if processed_files:
        ## Use existing processed file if available
        latest_processed_file = processed_files[-1]
        log.info(
            f"Using existing processed file: "
            f"{latest_processed_file.name}"
        ) 
        processed_path_df = latest_processed_file
    else:
        ## Run full pipeline from raw if no processed file
        log.info(
            "No processed file found. "
            "Running the files: extract, validate, and transform first"
        )
        raw_files = sorted(Path("data/raw").glob("*.csv"))
        if not raw_files:
            log.error(
                "No raw files found in data/raw/. "
                "Run extract.py first."
            )
            sys.exit(1)
        latest_raw = raw_files[-1]
        validated_df = validate_data.fn(latest_raw)
        processed_path_df = transform_data.fn(validated_df)
    
    ## Execute load.py ##
    log.info("Running Load Data...")
    partition_paths = load_data.fn(processed_path_df)

    ## === Display Summary === ##
    print("\n" + "=" * 55)
    print("LOAD SUMMARY")
    print("=" * 55)
    print(
        f"Total partitions created: "
        f"{len(partition_paths)}"
    )
    print()
    for market, path in partition_paths.items():
        df_part = pd.read_parquet(path)
        print(f" {market}")
        print(f" File   : {path.name}")
        print(f" Rows   : {len(df_part):,}")
        print(f" Columns  :  {len(df_part.columns)}")

        print(
            f" Date Range: "
            f"{df_part['date'].min().date()}"
            f"{df_part['date'].max().date()}"
        )
        print(
            f" Commodities: "
            f"{df_part['commodity'].unique().tolist()}"
        )
        print()
    print("=== Data Loading Finished ===")









