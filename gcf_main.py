# This is the main file used in Google Cloud Functions (GCF)

import functions_framework
import pandas as pd
import json
import requests
import sqlalchemy
from datetime import datetime, timedelta
from pytz import timezone

@functions_framework.http
def push_dynamic_tables_to_gcs(request): # the main function; specify as entry point in GUI
    print('Start!')

    config_file = "./db_config_gcs_gans.json"
    login_weather  = "./api_login_openweather.json"
    login_flights  = "./api_login_rapidapi.json"

    print(config_file, login_weather, login_flights)
    
    cities = get_cities_df(config_file)
    
    url, appid = get_api_login(login_weather)
    weather_df = get_weather_data(cities, url, appid)
    push_df_to_sql(weather_df, 'weather', config_file, return_df=False)
    
    c = cities.loc[cities.city.isin(['Berlin'])]
    airports_df = get_airports(c, login_flights)
    
    arrivals_df = get_flights_data(login_flights, airports_df, days=1)
    push_df_to_sql(arrivals_df, 'arrivals', config_file, return_df=False)

    return 'Success!'


# utils.py

def make_conn_str(config_file):
    """
    Creates a connection string for MySQL database using the configuration 
    parameters provided in a JSON configuration file.

    Parameters:
    - config_file (str): The path to the JSON configuration file containing 
      database connection parameters.

    Returns:
    - conn_str (str): The MySQL connection string formatted as 
      'mysql+pymysql://<user>:<password>@<host>:<port>/<schema>'.
    """
    with open(config_file) as f:
        config = json.load(f)
        schema = config['schema']
        host = config['host']
        user = config['user']
        port = config['port']
        db_password = config['db_password']
    return f'mysql+pymysql://{user}:{db_password}@{host}:{port}/{schema}'


def get_cities_df(config_file):
    """
    Retrieves a DataFrame containing unique city data from a MySQL database 
    using the configuration parameters provided in a JSON configuration file.
    Ensure that the 'cities' and 'geography' tables exist in the database and 
    have the necessary columns for the query to execute successfully.

    Parameters:
    - config_file (str): The path to the JSON configuration file containing 
      database connection parameters.

    Returns:
    - cities (DataFrame): A pandas DataFrame containing unique city data with 
      columns 'city_id', 'city', 'lat', and 'lon'.
    """
    # Obtain the connection string using make_conn_str function
    connection_string = make_conn_str(config_file)
    
    # Read data from MySQL database into a DataFrame
    cities = pd.read_sql('cities', con=connection_string)
    return cities


def push_df_to_sql(df, table_name, config_file, return_df=False):
    """
    Pushes a pandas DataFrame to a specified table in a MySQL database using 
    the configuration parameters provided in a JSON configuration file.
    Ensure that the table exists in the database and has the necessary columns 
    to accept the DataFrame data.

    Parameters:
    - df (DataFrame): The pandas DataFrame to be pushed to the database.
    - table_name (str): The name of the table in the database where the DataFrame
      will be inserted.
    - config_file (str): The path to the JSON configuration file containing 
      database connection parameters.
    - return_df (bool, optional): A boolean indicating whether to return the DataFrame
      read from the specified table in the database after the insertion. Default is False.

    Returns:
    - df_sql (DataFrame, optional): If return_df is True, returns a pandas DataFrame
      containing the data read from the specified table in the database after the insertion.
    """
    # Obtain the connection string using make_conn_str function
    connection_string = make_conn_str(config_file)
    
    # Push the DataFrame to the specified table in the MySQL database
    df.to_sql(table_name,
              if_exists='append',
              con=connection_string,
              index=False)

    if return_df:
        # Read the DataFrame from the specified table in the MySQL database
        df_sql = pd.read_sql(table_name, con=connection_string)
        return df_sql
        

def get_api_login(login_file):
    """
    Retrieves API login information from a JSON configuration file.

    Parameters:
    - login_file (str): The path to the JSON configuration file containing 
      API login information.

    Returns:
    - url (str): The URL for the API endpoint.
    - appid (str): The application ID for accessing the API.
    """
    with open(login_file) as f:
        login = json.load(f)
        url = login['url']
        appid = login['appid']
    return url, appid
    

def get_weather_data(cities_df, url, appid):
    """
    Retrieves weather data for cities from an external API and returns the data 
    as a pandas DataFrame.

    Parameters:
    - cities_df (DataFrame): A pandas DataFrame containing city data, including 
      columns 'city', 'lat', 'lon', and 'city_id'.
    - url (str): The base URL of the weather API endpoint.
    - appid (str): The application ID required for accessing the weather API.

    Returns:
    - weather_df (DataFrame): A pandas DataFrame containing weather data for the 
      specified cities, including columns 'city_id', 'retrieval_time', 
      'forecast_time', 'weather_desc', 'temp', 'temp_feels', 'pop', 'rain_mm', 
      and 'wind_speed'.
    """
    # Define Berlin timezone
    berlin_timezone = timezone('Europe/Berlin')
    
    # Initialize a dictionary to store weather data
    weather_dict = {
        'city_id': [],
        'retrieval_time': [],
        'forecast_time': [],
        'weather_desc': [],
        'temp': [],
        'temp_feels': [],
        'pop': [],
        'rain_mm': [],
        'wind_speed': [],
    }
    
    # Iterate through cities in the cities DataFrame
    for city in list(cities_df.city):
        print(city)
        
        # Get latitude, longitude, and city_id for the current city
        lat = cities_df.loc[cities_df.city == city, 'lat'].values[0]
        lon = cities_df.loc[cities_df.city == city, 'lon'].values[0]
        city_id = cities_df.loc[cities_df.city == city, 'city_id'].values[0]

        # Make a request to the weather API
        weather = requests.get(f'{url}lat={lat}&lon={lon}&appid={appid}&units=metric')
        
        # Parse the JSON response
        weather_json = weather.json()

        # Extract weather data and populate the dictionary
        for i in range(0, len(weather_json['list'])):
            weather_dict['city_id'].append(city_id)
            weather_dict['retrieval_time'].append(datetime.now(berlin_timezone)
                                                  .strftime("%Y-%m-%d %H:%M:%S"))
            weather_dict['forecast_time'].append(weather_json['list'][i]['dt_txt'])
            weather_dict['weather_desc'].append(weather_json['list'][i]['weather'][0]['description'])
            weather_dict['temp'].append(weather_json['list'][i]['main']['temp'])
            weather_dict['temp_feels'].append(weather_json['list'][i]['main']['feels_like'])
            weather_dict['pop'].append(weather_json['list'][i]['pop'])
            if 'rain' in weather_json['list'][i]:
                weather_dict['rain_mm'].append(weather_json['list'][i]['rain']['3h'])
            else:
                weather_dict['rain_mm'].append(0)
            weather_dict['wind_speed'].append(weather_json['list'][i]['wind']['speed'])

    # Create a DataFrame from the weather dictionary
    weather_df = pd.DataFrame(weather_dict)
    
    # Convert columns to datetime format
    weather_df['retrieval_time'] = pd.to_datetime(weather_df['retrieval_time'])
    weather_df['forecast_time'] = pd.to_datetime(weather_df['forecast_time'])

    return weather_df


def get_datetime_list(days=1):
    """
    Generates a list of datetime objects starting from tomorrow at midnight, 
    with entries every 12 hours, for a specified number of days.

    Parameters:
    - days (int): Number of days for which to generate datetime entries. Defaults to 1.

    Returns:
    - list: A list containing datetime objects starting from tomorrow at midnight, 
    with entries every 12 hours for the specified number of days.
    """
    # Get tomorrow's date
    tomorrow = datetime.now() + timedelta(days=1)

    # Set the time to midnight
    tomorrow_midnight = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)

    # Create a list to store the datetimes
    date_list = []

    # Generate datetimes starting from tomorrow at midnight with entries every 12 hours for n days
    for i in range(days * 3):
        date_list.append((tomorrow_midnight + timedelta(hours=12*i)))

    return date_list


def get_url_list(url_base, icao, date_list):
    """
    Generates a list of URLs based on the provided URL base, airport ICAO code, 
    and list of datetime objects.

    Parameters:
    - url_base (str): The base URL to which the airport ICAO code and datetime 
    ranges will be appended.
    - icao (str): The ICAO code of the airport.
    - date_list (list): A list containing datetime objects representing the 
    start and end times of intervals.

    Returns:
    - list: A list of URLs generated based on the provided parameters. Each URL 
    corresponds to a time interval specified in the date_list.
    """
    url_list = []

    # Generate URLs based on datetime intervals
    for i in range(0, len(date_list)-1):
        startdate = f'{date_list[i].date()}T{date_list[i].time().strftime("%H:%M")}'
        enddate = f'{date_list[i+1].date()}T{date_list[i+1].time().strftime("%H:%M")}'
        url_list.append(url_base + icao + '/' + startdate + '/' + enddate)

    return url_list


def get_airports(cities_df, login_file):
    """
    Retrieves airports information based on the provided cities DataFrame.

    Parameters:
    - cities_df (pandas DataFrame): A DataFrame containing information about cities 
    including latitude, longitude, and city ID.
    - login_file (str): The path to the JSON file containing login credentials 
    including 'X-RapidAPI-Key' and 'X-RapidAPI-Host'.

    Returns:
    - pandas DataFrame: A DataFrame containing airport information including ICAO 
    and IATA codes associated with cities.
    """
    with open(login_file) as f:
        login = json.load(f)
        key = login['X-RapidAPI-Key']
        host = login['X-RapidAPI-Host']

    airports_list = []
    url = "https://aerodatabox.p.rapidapi.com/airports/search/location"
    
    for idx, row in cities_df.iterrows():
        lat, lon, id = row['lat'], row['lon'], row['city_id']
        querystring = {"lat":lat,"lon":lon,"radiusKm":"50",
                   "limit":"5","withFlightInfoOnly":"true"}
        headers = {"X-RapidAPI-Key": key,
                   "X-RapidAPI-Host": host}
        response = requests.get(url, headers=headers, params=querystring)
        airports_df = pd.json_normalize(response.json()['items'])
        airports_df = airports_df[['icao', 'iata']]
        airports_df['city_id'] = id
        airports_list.append(airports_df)

    return pd.concat(airports_list, ignore_index=True)


def get_flights_data(login_file, airports_df, days=1):
    """
    Retrieves flight data for a specified airport (ICAO code) for a given number of days.

    Parameters:
    - login_file (str): The path to the JSON file containing login credentials including 
    'aeroflightbox_url_base', 'X-RapidAPI-Key', and 'X-RapidAPI-Host'.
    - icao (str): The ICAO code of the airport for which flight data is requested.
    - days (int): Number of days for which to retrieve flight data. Defaults to 1.

    Returns:
    - pandas DataFrame: A DataFrame containing flight arrival data including scheduled 
    times in UTC and local time.
    """
    with open(login_file) as f:
        login = json.load(f)
        url_base = login['aeroflightbox_url_base']
        key = login['X-RapidAPI-Key']
        host = login['X-RapidAPI-Host']

    arrival_times = []
    
    # Generate list of datetimes for specified number of days
    date_list = get_datetime_list(days=days)

    # Loop through all icao codes
    for idx, row in airports_df.iterrows():
        icao_code = row['icao']

        # Generate list of URLs for each datetime interval
        url_list = get_url_list(url_base, icao_code, date_list)
    
        # Retrieve arrival times from each URL
        for url in url_list:
            querystring = {"withLeg": "true", "direction": "Both", "withCancelled": "false",
                           "withCodeshared": "false", "withCargo": "false", "withPrivate": "false",
                           "withLocation": "false"}
    
            headers = {"X-RapidAPI-Key": key,
                       "X-RapidAPI-Host": host}

            response = requests.get(url, headers=headers, params=querystring)
    
            # Extract arrival times from the response
            for item in response.json()['arrivals']:
                arrival_times.append(pd.json_normalize(item['arrival']['scheduledTime']))
                # add icao code to extracted times
                arrival_times[-1]['arrival_airport_icao'] = icao_code
        
    # Concatenate arrival times into a DataFrame
    df = pd.concat(arrival_times, ignore_index=True)
    df['utc'] = pd.to_datetime(df['utc'])
    df['local'] = pd.to_datetime(df['local'])

    return df
