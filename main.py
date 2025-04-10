import pandas as pd
import gc
import daily_tableau
from config import COLS
from utils import get_processed_data, save_data_for_tableau
import os
import argparse
import logging
import warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def final_day_prediction_pipeline_for_tableau(hotel_id ):
    logger.info(f"Starting Data prep for hotel id: {hotel_id}")

    logger.info(f"Fetching the Data from s3")
    data = get_processed_data(hotel_id, 'processed_2d_data_with_final_day_predictions')

    logger.info("Data processing for additional features")
    data = daily_tableau.process_data_for_tableau(data, COLS)

    logger.info("Saving processed data...")
    save_data_for_tableau(data, hotel_id)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Hotel Data Processing Tableau Dashboard")
    parser.add_argument("--hotel_id", type=str, required=True, help="Hotel ID for which the data is processed")
    args = parser.parse_args()

    final_day_prediction_pipeline_for_tableau(args.hotel_id)
