import os
import pandas as pd
import numpy as np
import configparser
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf
from datasets.data import Data
from datetime import datetime

# config parser configuration
config = configparser.ConfigParser()
config.read_file(open("aws/credentials.cfg"))
# environ variables
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def spark_session():

    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.1.0-s_2.11")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5")\
        .enableHiveSupport()\
        .getOrCreate()

    return spark


def load_dim_tables(path, spark):
    """
    *** Code for DIM_US_CITY table. ***
    """
    # dim_us_city table
    us_city_path = path + "us-cities-demographics.csv"
    df_city = spark.read.format("csv").option(
        "header", "true").option("delimiter", ";").load(us_city_path)

    # drop duplicates
    columns = ['City', 'State', 'Male Population', 'Female Population', 'Total Population']
    df_city = df_city.select(*columns).dropDuplicates()

    # fill NaN values
    df_cityFill = df_city.fillna({'Male Population': 0, 'Female Population': 0})

    # add state column
    @udf
    def state_prefix(name):
        return [key for key, value in Data.states.items() if value == name][0]

    df_cityState = df_cityFill.withColumn("state_prefix", state_prefix(df_cityFill.State))

    # create a state and city variable for dim_city_temp table
    state_city = df_cityState.select("state_prefix", "City").dropDuplicates().rdd.map(
        lambda x: (x[0], x[1])).collect()

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

    """
    *** Code for DIM_COUNTRY table. ***
    """

    columns = ['id_country', 'country']
    df_country = pd.DataFrame([(key, value)
                               for key, value in Data.countries.items()], columns=columns)


def load_fact_table(path, spark):

    df = spark.read.parquet("sas_data")

    # change arrdate and depdate columns to datetype
    epoch = datetime(1960, 1, 1)
    sas_day = udf(lambda x: (timedelta(days=int(x)) + epoch) if x else None, T.DateType())
    df_dateParse = df.withColumn("arrdate", sas_day(df.arrdate))
    df_dateParse2 = df_dateParse.withColumn("depdate", sas_day(df_dateParse.depdate))

    # change dtaddto column to date if value if valid else return None
    def char_date(string):
        try:
            return datetime.strptime(str(sas), "%m%d%Y")
        except:
            return None

    udf_charDate = udf(char_date, T.DateType())
    df_charDate = df_dateParse2.withColumn("dtaddto", udf_charDate(df_dateParse2.dtaddto))

    # change double columns to IntegerType()
    to_int = F.udf(lambda x: int(x), T.IntegerType())
    columns = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'i94mode', 'arrdate',
               'depdate', 'dtaddto', 'i94addr', 'biryear', 'i94bir', 'gender', 'i94visa', 'visatype', 'airline']

    df_immigration = df_charDate.withColumn("cicid", to_int(df_charDate.cicid))\
        .withColumn("i94yr", to_int(df_charDate.i94yr))\
        .withColumn("i94mon", to_int(df_charDate.i94mon))\
        .withColumn("i94cit", to_int(df_charDate.i94cit))\
        .withColumn("i94res", to_int(df_charDate.i94res))\
        .withColumn("i94mode", to_int(df_charDate.i94mode))\
        .withColumn("biryear", to_int(df_charDate.biryear))\
        .withColumn("i94bir", to_int(df_charDate.i94bir))\
        .withColumn("i94visa", to_int(df_charDate.i94visa))\
        .select(*columns)

    # change name of columns of dataframe
    new_columns = ['id', 'year', 'month', 'citizen', 'resident', 'port_entry', 'mode_entry', 'arrival_date',
                   'dep_date', 'dateadd_to', 'state_addr', 'birth_year', 'age', 'gender', 'visa_code', 'visa_type', 'airline']
    df_imm = df_immigration.toDF(*new_columns)


def main():

    main_path = os.getcwd() + "/datasets/"
    output_data = "s3a://bucket-etl/"

    spark = spark_session()
    load_dim_tables(main_path, spark)
    load_fact_table(main_path, spark)


if __name__ == '__main__':
    main()
