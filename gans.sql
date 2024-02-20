-- Drop the database if it already exists
DROP DATABASE IF EXISTS gans;

-- Create the database
CREATE DATABASE gans;

-- Use the database
USE gans;

-- Create the 'cities' table
CREATE TABLE cities (
    city_id INT AUTO_INCREMENT, -- Automatically generated ID
    city VARCHAR(31) NOT NULL, -- Name of the city
    pop INT NOT NULL, -- population number
    lat float NOT NULL, -- latitude string
    lon float NOT NULL, -- longitude string
    country VARCHAR(31) NOT NULL, -- country name string
    PRIMARY KEY (city_id) -- Primary key to uniquely identify each city
);

SELECT * FROM cities;

-- Create the 'weather' table
CREATE TABLE weather (
    weather_id INT AUTO_INCREMENT, -- Automatically generated ID for each entry
    city_id INT, -- ID of the city
    retrieval_time DATETIME, -- retrieval time
    forecast_time DATETIME, -- forecast time
    weather_desc VARCHAR(31), -- weather description string
    temp float, -- air temperature
    temp_feels float, -- felt temperature
    pop float, -- probability of precipitation
    rain_mm float, -- rain in previous 3h
    wind_speed float, -- wind speed
    PRIMARY KEY (weather_id), -- Primary key to uniquely identify each entry
    FOREIGN KEY (city_id) REFERENCES cities(city_id) -- Foreign key to connect each coord to its city
);

SELECT * FROM weather;

-- Create the 'airports' table
CREATE TABLE airports (
	-- airport_id INT AUTO_INCREMENT, -- Automatically generated ID for each entry
    icao VARCHAR(7), -- icao code
    iata VARCHAR(7), -- iata code
	city_id INT, -- ID of the city where airport is located
	PRIMARY KEY (icao), -- Primary key to uniquely identify each entry
    FOREIGN KEY (city_id) REFERENCES cities(city_id) -- Foreign key to connect airport to its city
);

SELECT * FROM airports;

-- Create the 'arrivals' table
CREATE TABLE arrivals (
    arrival_id INT AUTO_INCREMENT, -- Automatically generated ID for each entry
    utc DATETIME, -- flight utc time
    local DATETIME, -- flight local time
    arrival_airport_icao VARCHAR(7), -- icao code
    PRIMARY KEY (arrival_id), -- Primary key to uniquely identify each entry
    FOREIGN KEY (arrival_airport_icao) REFERENCES airports(icao) -- Foreign key to connect each arrival to an airport
);

SELECT * FROM arrivals;