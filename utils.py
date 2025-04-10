import pandas as pd
import os 
from io import BytesIO
from dotenv import load_dotenv
import datetime
import boto3
import json
from datetime import datetime, timedelta

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')


def generate_stay_date_lag_features(processed_data, lag_columns , lag_days):
    df = processed_data.copy()

    for feature in lag_columns:
        for offset in lag_days[1:]:
            col_name =  f'{feature}_lag_{-offset}_-{offset}'

            offset_data = df[['report_date','stay_date', feature]].copy()
            offset_data = offset_data.rename(
                columns={'stay_date': 'offset_stay_date',
                        feature: col_name}
            )
            offset_data = offset_data.rename(
                columns={'stay_date': 'offset_stay_date',
                        feature: col_name}
            )
            df['lookup_stay_date'] = df['stay_date'] - pd.Timedelta(days=offset)
            df['lookup_report_date'] = df['report_date'] - pd.Timedelta(days=offset)


            df = pd.merge(
                df,
                offset_data,
                how='left',
                left_on = ['lookup_report_date','lookup_stay_date'],
                right_on= ['report_date','offset_stay_date']
            )
            
            df.drop(columns=['lookup_stay_date', 'offset_stay_date', 'lookup_report_date', 'report_date_y'], inplace=True)
            df = df.rename(columns={'report_date_x': 'report_date'})

    return df

def last_year_features(data, features, only_last_year_features = False):

    def get_last_year_date(date):
        # Get the ISO week number and day of the week
        iso_calendar = date.isocalendar()
        year, week_number, day_of_week = iso_calendar

        # Calculate the date for the same week number and day of week in the previous year
        last_year = year + 1
        last_year_date = pd.to_datetime(f"{last_year}-W{week_number:02d}-{day_of_week}", format="%G-W%V-%u")

        return last_year_date

    # Create copies of the dataframe for last year's data
    data_last_year = data.copy()

    # Map report_date and stay_date to last year
    data_last_year['report_date_last_year'] = data_last_year['report_date'].apply(get_last_year_date)
    data_last_year['stay_date_last_year'] = data_last_year['stay_date'].apply(get_last_year_date)
 
    # Rename the specified features for the last year data
    rename_dict = {feature: f"{feature}_last_year" for feature in features}
    last_year_features_name = list(rename_dict.values())
    data_last_year = data_last_year.rename(columns=rename_dict)

    # Merge the original data with the last year data
    result = pd.merge(
        data,
        data_last_year[['report_date_last_year', 'stay_date_last_year'] + list(rename_dict.values())],
        left_on=['report_date', 'stay_date'],
        right_on=['report_date_last_year', 'stay_date_last_year'],
        how='left'
    )

    result = result.drop(['report_date_last_year', 'stay_date_last_year'], axis=1)
    if only_last_year_features:
        result = result[['stay_date', 'report_date'] + last_year_features_name]
    return result


def get_latest_timestamp(prefix):
    timestamps = set()
    continuation_token = None

    while True:
        response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix,
            ContinuationToken=continuation_token
        ) if continuation_token else s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

        if 'Contents' not in response:
            break

        for obj in response['Contents']:
            parts = obj['Key'].split('/')
            if len(parts) == 4 or len(parts) == 5:
                if '_' in parts[2]:
                    timestamp = parts[2]
                else:
                    timestamp = parts[3]
                if timestamp.replace('_', '').isdigit():
                    timestamps.add(timestamp)

        # Check if more objects are available
        if response.get('IsTruncated'):  # There are more objects to fetch
            continuation_token = response['NextContinuationToken']
        else:
            break

    return max(timestamps) if timestamps else None


def get_processed_data(hotel_id, file_name):
    """Retrieve the latest processed data file for a given hotel_id and file_name."""
    prefix = f"processed_data/{hotel_id}/"
    latest_timestamp = get_latest_timestamp(prefix)

    if not latest_timestamp:
        raise FileNotFoundError(f"No processed data found for hotel_id {hotel_id}")
    
    s3_path = f"{prefix}{latest_timestamp}/{file_name}.parquet"
    
    
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_path)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"File not found: {s3_path}")
    

def get_tableau_data(hotel_id, file_name):
    """Retrieve the latest processed data file for a given hotel_id and file_name."""

    prefix = f"tableau_data/{hotel_id}"
    
    s3_path = f"{prefix}/{file_name}"
    
    
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_path)
        return pd.read_csv(BytesIO(response['Body'].read()))
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"File not found: {s3_path}")


def upload_to_s3(local_path, s3_path):
    try:
        s3.upload_file(local_path, BUCKET_NAME, s3_path)
        logger.info(f"Successfully uploaded {local_path} to {s3_path}")
    except Exception as e:
        logger.error(f"Error uploading {local_path} to S3: {str(e)}")


def save_and_upload(data, file_name, hotel_id, file_format='parquet'):
    timestamp = datetime.now().strftime("%Y_%m_%d")
    local_dir = f'tableau_data/{hotel_id}'
    s3_dir = f'tableau_data/{hotel_id}'
    os.makedirs(local_dir, exist_ok=True)
    
    local_path = f'{local_dir}/{file_name}.{file_format}'
    s3_path = f'{s3_dir}/{file_name}.{file_format}'
    
    if file_format == 'parquet':
        data.to_parquet(local_path, index=False)
    elif file_format == 'json':
        with open(local_path, 'w') as f:
            json.dump(data, f)
    elif file_format == 'csv':
        data.to_csv(local_path, index=False)

    upload_to_s3(local_path, s3_path)
    os.remove(local_path)


def save_data_for_tableau(data, hotel_id):
    save_and_upload(data, '2d_predictions', hotel_id, file_format='csv')
