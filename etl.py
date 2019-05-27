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


def load_dim_tables(path):
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

    return state_city


def main():

    main_path = os.getcwd() + "/datasets/"
    load_dim_tables(main_path)


if __name__ == '__main__':
    main()
