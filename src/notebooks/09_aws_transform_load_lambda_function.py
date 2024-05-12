from io import StringIO
import json

import boto3
import pandas as pd

def taxi_trips_transformations(taxi_trips: pd.DataFrame) -> pd.DataFrame:
    """Perform transformations with the taxi data.

    Parameters:
        taxi_trips (pd.DataFrame): The DataFrame holding the daily taxi trips.

    Returns:
        pd.DataFrame: The cleaned, transformed DataFrame holding the daily taxi trips.
    
    """

    if not isinstance(taxi_trips, pd.DataFrame):
        raise TypeError('taxi_trips is not a valid pandas DataFrame')

    taxi_trips.drop(['pickup_census_tract', 'dropoff_census_tract', 
                     'pickup_centroid_location', 'dropoff_centroid_location'], axis=1, inplace=True)

    taxi_trips.dropna(inplace=True)

    taxi_trips.rename(columns={'pickup_community_area': 'pickup_community_area_id', 
                            'dropoff_community_area': 'dropoff_community_area_id'}, inplace=True)

    taxi_trips['datetime_for_weather'] = pd.to_datetime(taxi_trips['trip_start_timestamp']).dt.floor('h')

    return taxi_trips
    
def update_master(taxi_trips: pd.DataFrame, map_table: pd.DataFrame, id_column: str, value_column: str) -> pd.DataFrame:
    """Extend the map table with new values if there are new values.

    Args:
        taxi_trips (pd.DataFrame): DataFrame holding the daily taxi trips.
        map_table (pd.DataFrame): DataFrame holding the master data.
        id_column (str): The id column of map table.
        value_column (str): Name of the column in map table containing the values.

    Returns:
        pd.DataFrame: The updated master data.
    """
    
    max_id = map_table[id_column].max()

    # new_values_list = [value for value in taxi_trips[value_column].values if value not in map_table[value_column].values]
    new_values_list = list(set(taxi_trips[value_column].values) - set(map_table[value_column].values))

    new_values_df = pd.DataFrame({
    id_column: range(max_id + 1, max_id + len(new_values_list) + 1),
    value_column: new_values_list

    })
    
    updated_map_table = pd.concat([map_table, new_values_df], ignore_index=True)

    return updated_map_table

def update_taxi_trips_wit_map_table(taxi_trips: pd.DataFrame, company_map_table: pd.DataFrame, payment_type_map_table: pd.DataFrame) -> pd.DataFrame:
    """Update the taxi trips DataFrame with company and payment type map table ids, and delete the string columns.

    Args:
        taxi_trips (pd.DataFrame): DataFrame holding the daily taxi trips.
        company_map_table (pd.DataFrame): The comapny map table.
        payment_type_map_table (pd.DataFrame): The payment type map table.

    Returns:
        pd.DataFrame: The taxi trips data with only payment type id and company id, with no company and payment type values.
    """
    taxi_trips_id = taxi_trips.merge(company_map_table, on='company')
    taxi_trips_id = taxi_trips_id.merge(payment_type_map_table, on='payment_type')

    taxi_trips_id.drop(['company', 'payment_type'], axis=1, inplace=True)

    return taxi_trips_id

def transform_weather_data(weather_data: json) -> pd.DataFrame:
    """Make transformations on daily weather api response.

    Args:
        weather_data (json): The daily weather data from Open-Meteo API.

    Returns:
        pd.DataFrame: A DataFrame representation of the data.
    """

    weather_data_filtered = {
        'datetime': weather_data['hourly']['time'],
        'temperature': weather_data['hourly']['temperature_2m'],
        'wind_speed': weather_data['hourly']['wind_speed_10m'],
        'rain': weather_data['hourly']['rain'],
        'precipitation': weather_data['hourly']['precipitation']
    }

    weather_df = pd.DataFrame(weather_data_filtered)
    weather_df['datetime'] = pd.to_datetime(weather_df['datetime'] )

    return weather_df

def read_csv_from_s3(bucket: str, path: str, filename: str) -> pd.DataFrame:
    """Downloads a csv file from an S3 bucket.
    
    Args:
        bucket (str): The bucket where the files at.
        path (str): The folder of the files.
        filename (str): Name of the file.

    Returns:
        pd.DataFrame: A DataFrame of the downloaded file.
    """
    
    s3 = boto3.client('s3')
    
    full_path = f'{path}{filename}'
    
    object = s3.get_object(Bucket=bucket, Key=full_path)
    object = object['Body'].read().decode('utf-8')
    output_df = pd.read_csv(StringIO(object))
    
    return output_df

def upload_dataframe_to_s3(dataframe: pd.DataFrame, bucket: str, path: str):
    """Uploads dataframe to the specified S3 path.
    
    Args:
        dataframe (pd.DataFrame): The dataframe to be uploaded.
        bucket (str): The bucket where the files at.
        path (str): The folder of the files.

    Returns:
        None
    
    """
    s3 = boto3.client('s3')
    
    buffer = StringIO()
    dataframe.to_csv(buffer, index=False)
    df_content = buffer.getvalue()
    s3.put_object(Bucket=bucket, Key=path, Body=df_content)


def upload_map_table_data_to_s3(bucket: str, path: str, file_type: str, dataframe: pd.DataFrame):
    """Uploads map table data(payment type or company) to S3. Copies the previous version and creates the new one.
    
    Args:
        bucket (str): The bucket where the files at.
        path (str): The folder of the files.
        file_type (str): Either company or payment_type.
        dataframe (pd.DataFrame): The dataframe to be uploaded.

    Returns:
        None
    
    """
    
    s3 = boto3.client('s3')
    
    map_table_file_path = f'{path}{file_type}_map_table.csv'
    previous_map_table_file_path =f'transformed_data/master_table_previous_version/{file_type}_map_table_previous_version.csv'
    
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': map_table_file_path}, Key=previous_map_table_file_path)
    
    upload_dataframe_to_s3(dataframe=dataframe, bucket=bucket, path=map_table_file_path)
    
def upload_and_move_file_on_S3 (dataframe: pd.DataFrame, datetime_col: str, bucket: str, 
        target_path_transformed: str, file_type: str, filename: str, source_path: str, target_path_raw: str):
    
    """Uploads a DataFrame to s3 and then moves a file from the base folder to another.
    
    Args:
        dataframe (pd.DataFrame), optional: The dataframe to be uploaded.
        datetime_col (str), optional: Datetime column name, which we derive the date for the filename.
        bucket (str): The bucket where the files at.
        target_path_transformed (str): Target path witihin the bucket where the transformed data would go.
        file_type (str): "weather" or "taxi"
        filename (str): Name of the file to be uploaded or moved
        sourve_path (str): Source path witihin the bucket
        target_path_raw (str): Target path witihin the bucket where the raw data would go.

    Returns:
        None
    
    """
    
    s3 = boto3.client('s3')
    
    formatted_date = dataframe[datetime_col].iloc[0].strftime('%Y-%m-%d')
    new_path_with_filename = f'{target_path_transformed}{file_type}_{formatted_date}.csv'
    
    upload_dataframe_to_s3(dataframe=dataframe, bucket=bucket, path=new_path_with_filename)
    
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': f'{source_path}{filename}'}, Key=f'{target_path_raw}{filename}')
    
    s3.delete_object(Bucket=bucket, Key=f'{source_path}{filename}')
    
    
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    bucket = 'chicago-taxi-data-project-dzs'
    raw_weather_folder = 'raw_data/to_processed/weather_data/'
    raw_taxi_trips_folder = 'raw_data/to_processed/taxi_data/'
    target_taxi_trips_folder = 'raw_data/processed/taxi_data/'
    target_weather_folder = 'raw_data/processed/weather_data/'
    
    transformed_taxi_trips_folder = 'transformed_data/taxi_trips/'
    transformed_weather_folder = 'transformed_data/weather/'
    
    payment_type_map_table_folder ='transformed_data/payment_type/'
    company_map_table_folder ='transformed_data/company/'
    
    payment_type_map_table_filename = 'payment_type_map_table.csv'
    company_map_table_filename = 'company_map_table.csv'
    
    payment_type_map_table = read_csv_from_s3(bucket=bucket, path=payment_type_map_table_folder, filename=payment_type_map_table_filename)
    company_map_table = read_csv_from_s3(bucket=bucket, path=company_map_table_folder, filename=company_map_table_filename)
    

    
    # Taxi data transformation and loading
    for file in s3.list_objects(Bucket=bucket, Prefix=raw_taxi_trips_folder)['Contents']:
        taxi_trips_key = file['Key']
        if taxi_trips_key.split('/')[-1].strip() == '':
            continue
        if taxi_trips_key.split('.')[1] == 'json':
            
            filename = taxi_trips_key.split('/')[-1]
            
            response = s3.get_object(Bucket=bucket, Key=taxi_trips_key)
            content = response['Body']
            taxi_trips_data_json = json.loads(content.read())
            
            taxi_trips_data_raw = pd.DataFrame(taxi_trips_data_json)
            taxi_trips_transformed = taxi_trips_transformations(taxi_trips_data_raw)
            
            company_map_table_updated = update_master(taxi_trips_transformed, company_map_table, 'company_id', 'company')
            payment_type_map_table_updated = update_master(taxi_trips_transformed, payment_type_map_table, 'payment_type_id', 'payment_type')
            
            taxi_trips = update_taxi_trips_wit_map_table(taxi_trips_transformed, company_map_table_updated, payment_type_map_table_updated)

            upload_and_move_file_on_S3 (dataframe=taxi_trips, datetime_col='datetime_for_weather', bucket=bucket, 
                target_path_transformed=transformed_taxi_trips_folder, file_type='taxi', filename=filename, 
                source_path=raw_taxi_trips_folder, 
                target_path_raw=target_taxi_trips_folder)
   
         
            upload_map_table_data_to_s3(bucket=bucket, path=company_map_table_folder, file_type='company', dataframe=company_map_table_updated)
            upload_map_table_data_to_s3(bucket=bucket, path=payment_type_map_table_folder, file_type='payment_type', dataframe=payment_type_map_table_updated)
            
    # Weather data transformation and loading
    for file in s3.list_objects(Bucket=bucket, Prefix=raw_weather_folder)['Contents']:
        weather_key = file['Key']
        if weather_key.split('/')[-1].strip() == '':
            continue
        if weather_key.split('.')[1] == 'json':
            
            filename = weather_key.split('/')[-1]
            
            response = s3.get_object(Bucket=bucket, Key=weather_key)
            content = response['Body']
            weather_data_json = json.loads(content.read())
            
            weather_data = transform_weather_data(weather_data_json)
            
            upload_and_move_file_on_S3 (dataframe=weather_data, datetime_col='datetime', bucket=bucket, 
                target_path_transformed=transformed_weather_folder, file_type='weather', filename=filename, 
                source_path=raw_weather_folder, 
                target_path_raw=target_weather_folder)
        
                
            # Upload to s3 function