import pandas as pd 
from datetime import datetime, timedelta
from utils import generate_stay_date_lag_features, last_year_features
from config import STAY_DATE_LAG_FEATURES, STAY_DATE_LAG_FEATURES_DAYS, DICT_RENAME
import logging
import warnings
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_data_for_tableau(processed_data, cols):
    report_date = datetime.now().date().strftime('%Y-%m-%d')
 
    processed_data = processed_data.query("lead_in >-2")
    processed_data.sort_values(by=['report_date', 'stay_date'], inplace= True)

    processed_data.competitor_median_rate = processed_data.groupby(['stay_date'])['competitor_median_rate'].ffill()
    processed_data.rate = processed_data.groupby(['stay_date'])['rate'].ffill()
    processed_data.pickup_total_revenue = processed_data.groupby(['stay_date'])['pickup_total_revenue'].ffill()
    processed_data.pickup_total_rooms = processed_data.groupby(['stay_date'])['pickup_total_rooms'].ffill()
    processed_data.pickup_adr = processed_data.groupby(['stay_date'])['pickup_adr'].ffill()

    fil_act = processed_data.query("lead_in >-1")
    fil_act['final_day_predicted_total_revenue_VAT_excluded'] = fil_act['predicted_revenue_VAT_excluded'] + fil_act['pickup_total_revenue_-1']
    fil_act['final_day_optimal_total_revenue_VAT_excluded'] = fil_act['optimal_revenue_VAT_excluded'] + fil_act['pickup_total_revenue_-1']

    processed_data = pd.merge(processed_data, fil_act[['report_date', 'stay_date', 'final_day_predicted_total_revenue_VAT_excluded', 'final_day_optimal_total_revenue_VAT_excluded']], 
                    on=['report_date', 'stay_date'], how= 'left')
                    

    last_year_features_list = [ 'rate', 'final_day_total_rooms_predicted', 'final_day_total_revenue_predicted', 'final_recorded_pickup_adr_predictions', 'optimal_price',
                                                    'final_day_predicted_total_revenue_VAT_excluded', 'revenue_uplift_VAT_excluded', 'final_day_optimal_total_revenue_VAT_excluded'
                                        ]

    logger.info(f"Generating stay date lag features")

    processed_data_laf = generate_stay_date_lag_features(processed_data, lag_columns = STAY_DATE_LAG_FEATURES, lag_days= STAY_DATE_LAG_FEATURES_DAYS)

    logger.info(f"Generating Last Year features")

    full_range_data = last_year_features(processed_data_laf, last_year_features_list, 
                                        only_last_year_features = False)

    logger.info(f"Generating monthly aggregated features")

    stay_date_df = pd.DataFrame(full_range_data.stay_date.unique(), columns=["stay_date"])

    past_stay_date_df = full_range_data.query("lead_in == -1")[["stay_date","pickup_total_revenue", 'pickup_adr', 'pickup_total_rooms']]

    future_stay_date_df = full_range_data.query("report_date == @report_date and lead_in >0")[["stay_date","pickup_total_revenue_-1",'predicted_revenue_VAT_excluded', 'pickup_adr_-1', 
                                                                                            'pickup_total_rooms_-1']]

    handle_stay_revenue = pd.to_datetime(timedelta(days=1) + pd.to_datetime(report_date))

    stay_date_df_first = stay_date_df.merge(past_stay_date_df, on = ["stay_date"], how="left")
    stay_date_df_second = stay_date_df_first.merge(future_stay_date_df, on = ["stay_date"], how = "left")

    stay_date_df_second.fillna(0, inplace=True)

    stay_date_df_second.loc[stay_date_df_second['stay_date'] != handle_stay_revenue, "Final Day Revenue on Book "] = (
        stay_date_df_second["pickup_total_revenue"] + stay_date_df_second["pickup_total_revenue_-1"]
    )

    stay_date_df_second.loc[stay_date_df_second['stay_date'] == handle_stay_revenue, "Final Day Revenue on Book "] = (
        stay_date_df_second["pickup_total_revenue"]
    )

    stay_date_df_second["Final Day Revenue Predicted"] =  stay_date_df_second["Final Day Revenue on Book "] + stay_date_df_second["predicted_revenue_VAT_excluded"]

    stay_date_df_second.loc[stay_date_df_second['stay_date'] != handle_stay_revenue, "Final Day ADR on Book"] = (
        stay_date_df_second["pickup_adr"] + stay_date_df_second["pickup_adr_-1"]
    )

    stay_date_df_second.loc[stay_date_df_second['stay_date'] == handle_stay_revenue, "Final Day ADR on Book"] = (
        stay_date_df_second["pickup_adr"]
    )

    stay_date_df_second.loc[stay_date_df_second['stay_date'] != handle_stay_revenue, "Final Day Rooms on Book"] = (
        stay_date_df_second["pickup_total_rooms"] + stay_date_df_second["pickup_total_rooms_-1"]
    )

    stay_date_df_second.loc[stay_date_df_second['stay_date'] == handle_stay_revenue, "Final Day Rooms on Book"] = (
        stay_date_df_second["pickup_total_rooms"]
    )

    data_with_revenue = pd.merge(full_range_data[cols], stay_date_df_second[['stay_date', 'Final Day Revenue on Book ', 'Final Day Revenue Predicted', 'Final Day ADR on Book', 'Final Day Rooms on Book']], on =['stay_date'], how = 'left')

    
    fil_all_features = data_with_revenue.query("report_date >= '2024-01-01' and report_date <= @report_date and lead_in > -2 ")[cols + [  'Final Day Revenue on Book ','Final Day Revenue Predicted', 'Final Day ADR on Book', 'Final Day Rooms on Book']].sort_values(by=['report_date', 'stay_date'])
    fil_all_features.rename(columns= DICT_RENAME, inplace=True)

    float_cols = fil_all_features.select_dtypes(include=['float64', 'float32']).columns
    fil_all_features[float_cols] = fil_all_features[float_cols].round(2)
    return fil_all_features
