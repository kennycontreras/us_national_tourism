import os
import pandas as pd
import numpy as np
import configparser
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf
from datasets.data import Data
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
from itertools import chain
from sql_queries import *

# config parser configuration
config = configparser.ConfigParser()
config.read_file(open("aws/credentials.cfg"))
# environ variables
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["SPARK_CLASSPATH"] = '~/Documents/jars/postgresql-9.3-1101.jdbc41.jar'

# global variables
total_imm_df = 0


def spark_session():

    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.1.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()

    return spark


def load_dim_tables(path, spark, output, url_db, properties):
    """
    *** Code for DIM_US_CITY table. ***
    """

    print("\nExecuting Dimension tables process.\n")
    print("This task may take a moment, please wait...")
    # dim_us_city table
    us_city_path = path + "us-cities-demographics.csv"
    df_city = spark.read.format("csv").option(
        "header", "true").option("delimiter", ";").load(us_city_path)

    # drop duplicates and change name of columns replacing spaces to `_`
    columns = ['City', 'State', 'Male Population', 'Female Population', 'Total Population']
    new_columns = [column.replace(" ", "_").lower() for column in columns]
    df_city = df_city.select(*columns).dropDuplicates().toDF(*new_columns)

    # fill NaN values
    df_cityFill = df_city.fillna({'male_population': 0, 'female_population': 0})

    # add state column
    @udf
    def state_prefix(name):
        return [key for key, value in Data.states.items() if value == name][0]

    df_cityState = df_cityFill.withColumn("state_prefix", state_prefix(df_cityFill.state))

    # write into postgresql
    df_cityState.write.mode("overwrite").jdbc(
        url=url_db, table="dim_us_city", properties=properties)

    # create a state and city variable for dim_city_temp table
    state_city = df_cityState.select("state_prefix", "city").dropDuplicates().rdd.map(
        lambda x: (x[0], x[1])).collect()

    """
    *** Code for DIM_CITY_TEMP table. ***
    """

    # list of cities of US based on a dictionary of main dataset source
    df_cities = pd.read_csv(path + "historical-hourly-weather-data/city_attributes.csv")
    us_cities = df_cities[df_cities.Country == "United States"]["City"].values.tolist()

    # Spark DataFrame for dim table, select columns based on us_cities list created before.
    path_temp = path + "historical-hourly-weather-data/temperature.csv"
    df = spark.read.format("csv").option("header", "true").load(path_temp)
    df_city = df.select('datetime', *us_cities)

    # Unpivot DataFrame
    df_newColumn = df_city.toDF(*[column.replace(" ", "") for column in df_city.columns])
    stack_statement = "stack(27, 'Portland', Portland, 'SanFrancisco', SanFrancisco, 'Seattle', Seattle, 'LosAngeles', LosAngeles, 'SanDiego', SanDiego, 'LasVegas', LasVegas, 'Phoenix', Phoenix, 'Albuquerque', Albuquerque, 'Denver', Denver, 'SanAntonio', SanAntonio, 'Dallas', Dallas, 'Houston', Houston, 'KansasCity', KansasCity, 'Minneapolis', Minneapolis, 'SaintLouis', SaintLouis, 'Chicago', Chicago, 'Nashville', Nashville, 'Indianapolis', Indianapolis, 'Atlanta', Atlanta, 'Detroit', Detroit, 'Jacksonville', Jacksonville, 'Charlotte', Charlotte, 'Miami', Miami, 'Pittsburgh', Pittsburgh, 'Philadelphia', Philadelphia, 'NewYork', NewYork, 'Boston', Boston) as (City, Temp)"

    df_weather = df_newColumn.selectExpr("Datetime", stack_statement).where("Temp is not null")

    # Change dateformat for Datetime column and order dataframe by datetime and city.
    datetime_udf = udf(lambda x: parse(x), T.DateType())

    df_weatherDate = df_weather.withColumn("Datetime", datetime_udf(df_weather.Datetime))\
        .orderBy("Datetime", "City")

    # Avg temperature column by datetime and city.
    df_weatherAvg = df_weatherDate.groupBy("Datetime", "City").agg({"Temp": "avg"})

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

    df_weatherReplace = df_weatherAvg.withColumn("City", replace_city(df_weatherAvg.City))

    # Add state prefix column
    @udf
    def state(name):
        for value in state_city:
            if name == value[1]:
                return value[0]  # value[0] is equal to state prefix
        return None  # if there's no match for City return None

    df_weatherState = df_weatherReplace.withColumn("state", state(df_weatherReplace.City))

    # rename and lower columns
    columns = [column.lower() for column in df_weatherState.columns]
    df_weatherLower = df_weatherState.toDF(*columns)

    # change temperature measurement from Kelvin to Fahrenheit
    fahrenheit_udf = F.udf(lambda x: '%.3f' % ((x - 273.15) * 1.8000 + 32.00)
                           )  # return a three decimal float number

    df_weatherFahrenheit = df_weatherLower.withColumn(
        "avg(temp)", fahrenheit_udf(F.col("avg(temp)")))

    # Write into postgres
    df_weatherFahrenheit.withColumnRenamed("avg(temp)", "temp").write.mode("overwrite").jdbc(
        url=url_db, table="dim_us_weather", properties=properties)

    """
    *** Code for DIM_AIRPORT table. ***
    """

    airport_path = path + "airport-codes_csv.csv"
    df = spark.read.format("csv").option("header", "true").load(airport_path)

    # filter dataset by country (US) and municipality not null
    df_airport = df.filter('iso_country = "US" and municipality is not null').select(
        'ident', 'type', 'name', 'iso_country', 'iso_region', 'municipality')

    # create state column using iso_region column
    udf_state = udf(lambda x: x[3::])

    df_airport_state = df_airport.withColumn("iso_region", udf_state(df_airport.iso_region))

    # change name of columns
    columns = ['id_airport', 'type', 'name', 'country', 'state', 'city']
    df_airportNew = df_airport_state.toDF(*columns)

    # write into postgres
    df_airportNew.write.mode("overwrite").jdbc(
        url=url_db, table="dim_airport", properties=properties)

    """
    *** Code for DIM_COUNTRY table. ***
    """

    columns = ['id_country', 'country']
    df_country = spark.createDataFrame([(key, value)
                                        for key, value in Data.countries.items()], schema=columns)

    df_country.write.mode("overwrite").jdbc(url=url_db, table="dim_country", properties=properties)
    print("Process completed.-")


def load_fact_table(path, spark, output, url_db, properties):
    global total_imm_df  # variable for data quality process

    print("\nExecuting Fact table process.\n")
    df = spark.read.parquet(path + "sas_data")

    print("Data cleaning process, this may take a moment, please wait...")
    # change arrdate and depdate columns to datetype

    epoch = datetime(1960, 1, 1)
    sas_day = udf(lambda x: (timedelta(days=int(x)) + epoch) if x else None, T.DateType())
    df_dateParse = df.withColumn("arrdate", sas_day(df.arrdate))
    df_dateParse2 = df_dateParse.withColumn("depdate", sas_day(df_dateParse.depdate))

    # change dtaddto column to date if value if valid else return None
    def char_date(date):
        try:
            return datetime.strptime(str(date), "%m%d%Y")
        except:
            return None

    udf_charDate = udf(char_date, T.DateType())
    df_charDate = df_dateParse2.withColumn("dtaddto", udf_charDate(df_dateParse2.dtaddto))

    # change double columns to IntegerType()
    to_int = F.udf(lambda x: int(x) if x else None, T.IntegerType())
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

    print("Writing into immigration_us table.")
    # write into postgres
    df_imm.write.mode("append").jdbc(url=url_db, table="immigration_us", properties=properties)
    print("Process completed.-")
    # Variable which contains the total rows for the Dataframe, this variable will be useful for a Data Quality process
    total_imm_df = df_imm.count()


def data_quality(cur):

    # first data quality
    print("\n1. Executing first data quality check\n")
    cur.execute(select_imm_count)
    count = cur.fetchall()

    if total_imm_df == count[0][0]:
        print("Data Quality check passed Successfully!\n\nTotal values in dataframe: {}\nTotal values in table: {}".format(
            total_imm_df, count[0][0]))
    else:
        print("Data Quality check failed! \nTotal values in dataframe: {}\n Total values in table: {}".format(
            total_imm_df, count[0][0]))

    # second data quality
    print("\n1. Executing second data quality check\n")
    cur.execute(select_arrival_date)
    result = cur.fetchall()

    if result[0][1] > 0 and result[0][1] > result[0][0]:
        print("Data Quality check passed Successfully!\n\nTotal null values: {}\nTotal not null values: {}".format(
            result[0][0], result[0][1]))
    else:
        print("Data Quality check failed!\n\nTotal null values: {}\n Total not null values: {}".format(
            result[0][0], result[0][1]))

    # third data quality
    print("\n1. Executing third data quality check\n")
    cur.execute(select_year_imm)
    result_1 = cur.fetchall()

    # check for distint years in dim_us_temp table
    cur.execute(select_year_weather)
    result_2 = cur.fetchall()

    if result_1[0][0] in chain.from_iterable(result_2):
        print("Data Quality check passed Successfully!")
    else:
        print("Data Quality check failed!")


def main():

    main_path = os.getcwd() + "/datasets/"
    output_data = "s3a://bucket-etl/capstone/"

    # spark config connection with postgres db
    url_db = "jdbc:postgresql://127.0.0.1:5432/imm_dwh"
    properties = {"user": "student", "password": "student", "driver": "org.postgresql.Driver"}

    # postgres connection
    conn = psycopg2.connect("host=127.0.0.1 dbname=imm_dwh user=student password=student")
    cur = conn.cursor()

    spark = spark_session()
    # load_dim_tables(main_path, spark, output_data, url_db, properties)
    load_fact_table(main_path, spark, output_data, url_db, properties)
    data_quality(cur)

    # commit and close connection
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
