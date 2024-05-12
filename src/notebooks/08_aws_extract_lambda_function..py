import boto3
import requests


# 1. DONE - Get T-2 months taxi data
# 2. DONE - Get T-2 months weather data
# 3. DONE - Upload to s3 (raw_data/to_processed/taxi_data or raw_data/to_processed/weather_data)
# 4. DONE - Create functions - organize the code
# 5. Creating a trigger


def get_taxi_data(formatted_datetime: str) -> List:
    """
    Retrieve taxi data for the given date.
    
    Parameters:
        formatted_datetime (str): The date in 'YYYY-MM-DD' format.
        
    Returns:
        Dict: A dictionary containing the taxi data as a JSON.
        
    """
    taxi_url = f"https://data.cityofchicago.org/resource/ajtu-isnz.json?$where=trip_start_timestamp >= '{formatted_datetime}T00:00:00' AND trip_start_timestamp <= '{formatted_datetime}T23:59:59'&$limit=30000"
    headers = {"X-App-Token": os.environ.get("CHICAGO_API_TOKEN")}
    taxi_response = requests.get(taxi_url)
    taxi_data = taxi_response.json()
    
    return taxi_data
    

def get_weather_data(formatted_datetime: str) -> Dict:
    """
    Retrieve weather data from Open Meteo API for a specified datetime.

    Parameters:
        formatted_datetime (str): The date in 'YYYY-MM-DD' format.

    Returns:
        Dict: A dictionary containing weather data for the specified datetime,
            including temperature at 2 meters above ground level,
            wind speed at 10 meters above ground level,
            rain, and precipitation.
    """
    weather_url = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        'latitude': 41.85,
        'longitude': -87.65,
        'start_date': formatted_datetime,
        'end_date': formatted_datetime,
        'hourly': 'temperature_2m,wind_speed_10m,rain,precipitation'
    }
    
    weather_response = requests.get(weather_url, params=params)
    weather_data = weather_response.json()
    
    return weather_data


def upload_to_s3(data: Dict, folder_name: str, filename: str) -> None:
    """
    Upload data to an Amazon S3 bucket.

    Parameters:
        data (Dict): The dictionary object to be uploaded, either taxi or weather data.
        folder_name (str): The name of the folder within the S3 bucket where the file will be stored.
        filename (str): The name of the file to be stored in the specified folder.

    Returns:
        None
    """
    client = boto3.client('s3')
    client.put_object(
        Bucket='chicago-taxi-data-project-dzs',
        Key=f"raw_data/to_processed/{folder_name}/{filename}",
        Body=json.dumps(data)
        )


def lambda_handler(event, context):
    current_datetime = datetime.now() - relativedelta(month=2)
    formatted_datetime = current_datetime.strftime('%Y-%m-%d')
    
    taxi_data = get_taxi_data(formatted_datetime)
    weather_data = get_weather_data(formatted_datetime)
    
    taxi_filename = f"taxi_raw_{formatted_datetime}.json"
    weather_filename = f"weather_raw_{formatted_datetime}.json"
   
    upload_to_s3(data=taxi_data, filename=taxi_filename, folder_name='taxi_data')
    print('Taxi data has been uploaded')
   
    upload_to_s3(data=weather_data, filename=weather_filename, folder_name='weather_data')
    print('Weather data has been uploaded')