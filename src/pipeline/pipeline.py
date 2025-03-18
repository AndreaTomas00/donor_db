import argparse
import os
import shutil
import time

from loguru import logger

from src.config import DATA_ROOT
from src.features.utils import load_params
from src.pipeline.extract import extract_data
from src.pipeline.load import insert_data
from src.pipeline.transform import transform_df


def pipeline_db(start_date="01/01/2025", end_date=None):
    """
    Pipeline for the donor database in dense format.
    """
    extract_params = load_params("extract")
    consultas = extract_params["consultas"]
    time_start = time.time()
    dfs = extract_data(consultas, start_date=start_date, end_date=end_date)
    time_end = time.time()
    logger.info(f"Data extraction took {time_end - time_start} seconds")
    os.makedirs(DATA_ROOT / "tmp", exist_ok=True)
    for name, df in dfs.items():
        df.to_pickle(DATA_ROOT / "tmp" / f"{name}.pkl")
    db = transform_df(dfs)
    os.makedirs(DATA_ROOT / "transformed", exist_ok=True)
    for name, df in db.items():
        df.to_pickle(DATA_ROOT / "transformed" / f"{name}.pkl")
    
    if insert_data(db):
        logger.info("Pipeline executed successfully")

        logger.info("Cleaning up temporary files...")
        shutil.rmtree(DATA_ROOT / "tmp", ignore_errors=True)
        shutil.rmtree(DATA_ROOT / "transformed", ignore_errors=True)
        logger.info("Temporary files removed")


def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description='Run donor database pipeline')
    parser.add_argument('--start_date', default='01/01/2025',
                      help='Start date for the data extraction (default: 01/01/2025)')
    parser.add_argument('--end_date', default=None,
                      help='End date for the data extraction (default: None)')
    
    # Parse arguments
    args = parser.parse_args()

    # Run the pipeline
    logger.info("Ejecutando pipeline...")
    pipeline_db(start_date=args.start_date, end_date=args.end_date)

if __name__ == "__main__":
    main()