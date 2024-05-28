# Data analysis of Chicagi taxi trips
This project aims to analyze Chicago taxi trips data alongside weather data to uncover insights and patterns. The data includes details such as trip miles, total fare, company, payment type, and pickup and dropoff locations, and it is combined with weather data from a separate API to provide a comprehensive view of taxi usage in different weather conditions.

## Introduction
This project involves fetching taxi trip data and weather data from two separate web APIs, processing the data using pandas, and creating visualizations to identify patterns and insights. The primary goal is to understand how various factors, including weather, affect taxi trips in Chicago. The data collection and storage are managed using AWS Lambda functions and stored in an S3 bucket.

## Data sources
1. __Chicago Taxi Trips Data:__ The data was gathered from Chicago Data Portal (https://data.cityofchicago.org/). Contains information on trip miles, total fare, company, payment type, pickup and dropoff locations etc.
2. __Weather Data:__ The data was gathered from the website: https://open-meteo.com/ Provides weather information such as temperature, precipitation, and wind speed for the corresponding dates of the taxi trips.

## Data processing
1. Fetching Taxi Trips Data: The script uses boto3 to access the S3 bucket and fetch the taxi trips data, loading it into a pandas DataFrame.
2. Fetching Weather Data: Similarly, the script fetches weather data from the S3 bucket and loads it into another DataFrame.
3. Merging Data: The taxi trips data and weather data are merged based on the date to create a comprehensive dataset.
4. Cleaning Data: The script includes steps to clean and preprocess the data, such as handling missing values and converting data types.

## Notebooks
1. __json_scraping:__ JSON (JavaScript Object Notation) is a popular format for transmitting data over the web, and it is widely used in APIs. This notebook will walk you through the process of fetching JSON data from web APIs and parsing it.
2. __web_scraping:__ Web scraping is the process of extracting data from websites, and BeautifulSoup is a powerful tool for parsing HTML and XML documents. This notebook will guide you through the entire process, from making HTTP requests to parsing and extracting and storing the data. In taxi trips dataset community areas are represented by numbers. I was gathered community areas names from wikipedia.
3. __get_taxi_data:__ This notebook gather taxi trips data from web API.
4. __get_weather_data:__ This notebook gather weather data from web API and store it in pandas DataFrame.
5. __date_dimension:__ This notebook creates date dimension table.
6. __trips_data_mapping:__ This notebbok gather taxi trips data from web API and store it in pandas DataFrame. Notebook contains data processing (data type conversions, handling NaN values etc.), checking joining taxi trips data and weather data, and creting payment type and company map table.
7. __transfrom_load:__ This notebook contains functions for taxi trips data transformation, updating of payment type and company map tables with new data, updating taxi trips data with the most recent payment type and company map tables and weather data transformation.
8. __aws_extract_lambda_function:__ This notebook contains functions for gathering taxi and weather data throug AWS lambda function, upload it to S3 bucket and lambda handler function.
9. __aws_transform_load_lambda_function:__ This notebook contains functions for taxi trips and weather data transformation, update map table, update taxi trips data with new map tables, read csv form S3, upload dataframe to S3, upload map table to S3, upload and move file on S3 and lambda handler function.
10. __local_visualisations:__ This notebook contains sime visualisation from data.

    Detecting outliers by box plot:
    ![image](https://github.com/Zsolt-d/Chicago_data_engineering/assets/151520036/c977568e-606d-41b0-8b30-8b3c9b666207)

    This bar plot represents how many trips each company made:
    ![image](https://github.com/Zsolt-d/Chicago_data_engineering/assets/151520036/0224c7ba-4800-445f-8478-3e3abf7f0aa4)

    This chart shows how many trips were on which day:
    ![image](https://github.com/Zsolt-d/Chicago_data_engineering/assets/151520036/6552e677-b78c-4ad9-810e-d8acc8675fef)

    This figure shows how many times the passengers paid with which payment type:
    ![image](https://github.com/Zsolt-d/Chicago_data_engineering/assets/151520036/4516b757-2ab4-45c7-8b17-fe4a0c28aad3)

    This bar plot represents the average trip total fare dependong on payment type:
    ![image](https://github.com/Zsolt-d/Chicago_data_engineering/assets/151520036/b1504b89-40f4-48ac-bd11-cefc016df85b)

## Conclusion
The project successfully integrated Chicago taxi trip data with weather data through a robust data engineering process. This involved collecting data from multiple APIs, cleaning and preprocessing the data, and merging it into a unified dataset. The comprehensive data engineering pipeline ensured that the data was accurate, consistent, and ready for analysis.



    


    

