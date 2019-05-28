import os
import pandas as pd
import numpy as np
import configparser
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf
from datasets.data import Data


def spark_session():

    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.1.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()

    return spark


def load_dim_tables(path, spark):
    """
    *** Code for DIM_US_CITY table. ***
    """
    # dim_us_city table
    us_city_path = path + "us-cities-demographics.csv"
    df_city = pd.read_csv(us_city_path, delimiter=";")

    # drop duplicates
    df_city[['City', 'State', 'Male Population',
             'Female Population', 'Total Population']].drop_duplicates()

    # fill NaN values
    df_city.fillna({'Male Population': 0, 'Female Population': 0}, inplace=True)

    # add state column
    def state_prefix(name):
        prefix = next(key for key, value in Data.states.items() if value == name)
        return prefix

    df_city['state_prefix'] = df_city['State'].apply(lambda x: state_prefix(x))

    # create a state and city variable for dim_city_temp table
    state_city = df[['state_prefix', 'City']].drop_duplicates().values.tolist()

    """
    *** Code for DIM_CITY_TEMP table. ***
    """

    # list of cities of US based on a dictionary of main dataset source
    df_cities = pd.read_csv(main_path + "historical-hourly-weather-data/city_attributes.csv")
    us_cities = df_cities[df_cities.Country == "United States"]["City"].values.tolist()

    # Spark DataFrame for dim table, select columns based on us_cities list created before.
    path_temp = main_path + "historical-hourly-weather-data/temperature.csv"
    df = spark.read.format("csv").option("header", "true").load(path_temp)
    df_city = df.select('datetime', *us_cities)

    # Unpivot DataFrame
    df_newColumn = df_city.toDF(*[column.replace(" ", "") for column in df_city.columns])
    stack_statement = "stack(27, 'Portland', Portland, 'SanFrancisco', SanFrancisco, 'Seattle', Seattle, 'LosAngeles', LosAngeles, 'SanDiego', SanDiego, 'LasVegas', LasVegas, 'Phoenix', Phoenix, 'Albuquerque', Albuquerque, 'Denver', Denver, 'SanAntonio', SanAntonio, 'Dallas', Dallas, 'Houston', Houston, 'KansasCity', KansasCity, 'Minneapolis', Minneapolis, 'SaintLouis', SaintLouis, 'Chicago', Chicago, 'Nashville', Nashville, 'Indianapolis', Indianapolis, 'Atlanta', Atlanta, 'Detroit', Detroit, 'Jacksonville', Jacksonville, 'Charlotte', Charlotte, 'Miami', Miami, 'Pittsburgh', Pittsburgh, 'Philadelphia', Philadelphia, 'NewYork', NewYork, 'Boston', Boston) as (City, Temp)"

    df_weather = df_newColumn.selectExpr("Datetime", stack_statement).where("Temp is not null")

    # Change dateformat for Datetime column and order dataframe by datetime and city.
    datetime_udf = udf(lambda x: parse(x), T.DateType())

    df_weatherdate = df_weather.withColumn("Datetime", datetime_udf(df_weather.Datetime))\
        .orderBy("Datetime", "City")

    # Avg temperature column by datetime and city.
    df_avg_weather = df_weatherDate.groupBy("Datetime", "City").agg({"Temp": "avg"})

    # Return name of cities to normal (spaces between words)
    replace_cities = {
        'SanFrancisco': 'San Francisco',
        'LosAngeles': 'Los Angeles',
        'SanDiego': 'San Diego',
        'LasVegas': 'Las Vegas',
        'SanAntonio': 'San Antonio',
        'KansasCity': 'Kansas City',
        'SaintLouis': 'Saint Louis',
        'NewYork': 'New York',
    }

    @udf
    def replace_city(name):
        for key, value in replace_cities.items():
            if name == key:
                return value
        return name

    df_weatherReplace = df_avg_weather.withColumn("City", replace_city(df_avg_weather.City))

    # Add state prefix column
    @udf
    def state(name):
        for value in state_city:
            if name == value[1]:
                return value[0]  # value[0] is equal to state prefix
        return None  # if there's no match for City return None

    df_weatherState = df_weatherReplace.withColumn("State", state(df_weatherReplace.City))

    # change temperature measurement from Kelvin to Fahrenheit
    fahrenheit_udf = F.udf(lambda x: '%.3f' % ((x - 273.15) * 1.8000 + 32.00)
                           )  # return a three decimal float number

    df_weatherFahrenheit = df_weatherState.withColumnRenamed(
        "avg(Temp)", "Temp").withColumn("Temp", fahrenheit_udf(df_weatherState.Temp))

    # Write parquet files
    df_weatherFahrenheit.write.partitionBy("State").parquet("weather.parquet")

    """
    *** Code for DIM_AIRPORT table. ***
    """

    airport_path = path + "airport-codes_csv.csv"
    df = spark.read.format("csv").option("header", "true").load(
        os.getcwd() + "/datasets/airport-codes_csv.csv")

    # filter dataset by country (US) and municipality not null
    df_airport = df.filter('iso_country = "US" and municipality is not null').select(
        'ident', 'type', 'name', 'iso_country', 'iso_region', 'municipality')

    # create state column using iso_region column
    udf_state = udf(lambda x: x[3::])

    df_airport_state = df_airport.withColumnRenamed("iso_region", "state")\
        .withColumn("state", udf_state(df_airport.state))


def main():

    main_path = os.getcwd() + "/datasets/"

    spark = spark_session()
    load_dim_tables(main_path, spark)


if __name__ == '__main__':
    main()
