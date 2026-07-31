"""
About run.py: This file executes the complete data pipeline end to end
with a single command. It connects all the four stages into a single 
reprpducible, observable, and resilient flow.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, get_run_logger

from pipeline.extract import extract_data
from pipeline.validate import validate_data
from pipeline.transform import transform_data
from pipeline.load import load_data

@flow(
    name="MarketFed-pipeline",
    log_prints=True,
    description=(
        "Full MarketFed data engineering pipeline: "
        "extract -- validate -- transform -- load. "
        "Produces federated client partitions for 3 Nigerian grain markets"
    ),
)
def run_pipeline() -> dict[str, Path]:
    """Coonect the ETL and validation stages in sequence and returns a dictionary
    that maps market name to a federated client partition path.
    """
    try:
        logger = get_run_logger()
    except Exception:
        logger = logging.getLogger(__name__)

    ## -- Flow Header --##
    run_time_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("=" * 35)
    logger.info("MarketFed Data Pipeline Started")
    logger.info(f"Run timestamp : {run_time_stamp}")
    logger.info("=" * 35)

    ## --- Stage: Extract ---##
    logger.info("Stage 1 of 4 - extract_data()")
    raw_path = extract_data()
    logger.info(f" Raw file : {raw_path.name}")

    ## --- Stage: Validate --- ##
    logger.info("Stage 2 of 4 - validate_data()")
    data_frame_validated = validate_data(raw_path)
    logger.info(
        f" Validated rows : {len(data_frame_validated):,}"
    )

    ## --- Stage: Transform --- ##
    logger.info("Stage 3 of 4 - transform_data()")
    processed_path = transform_data(data_frame_validated)
    logger.info(f" Processed file: {processed_path.name}")

    ## --- Stage: Load --- ##
    logger.info("Stage 4 of 4 - load_data()")
    partition_paths = load_data(processed_path)
    logger.info(f" Partitions created : {len(partition_paths)}")

    for market, path in partition_paths.items():
        logger.info(f" {market:<10} -> {path.name}")

    logger.info("=" * 35)
    logger.info("MarketFed Data Pipeline Completed")
    logger.info("=" * 35)

    return partition_paths
### ===== Standalone Execution ===== ###
if __name__ == "__main__":
    logging.basicConfig(
        level = logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )
    log = logging.getLogger(__name__)
    log.info("Starting MarketFed Data Pipeline....")

    
    ## Run the full prefect flow ##
    partition_paths = run_pipeline()

    ## Display final summary ##
    print("\n" + "=" * 35)
    print("Pipeline Complete")
    print("=" * 35)
    print(f"Federated client partitions created: {len(partition_paths)}")
    print()
    for market, path in partition_paths.items():
        print(f" {market: <10} -> {path}")
    print("=" * 35)
