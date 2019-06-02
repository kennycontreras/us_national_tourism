# US National Tourism Data Warehouse from Scratch

### Project Summary

This project was built with the goal of providing wide information for analyst purpose about Immigration data of US. This Datawarehouse provides clean and structured information about what kind of visa every immigrant has, which states or port entries receive more immigrants, how long it is going to be the period of stay in the US, visa types, etc.

With this information, you can provide an exhaustive analysis about e.g Which state receive more immigrants, which season is the best for travelers based on weather temperature of each state. How many people live in every state or city, airports for each state. You can even apply prediction models to know how many immigrants will the US receive on a specific day based on weather conditions.

![img](https://imgur.com/t7AQGIr.png)

## Introduction

The scope of this project is to gather valuable information related to Immigrants and build a structured Datawarehouse that can be helpful to Business Analytics and Machine Learning models.

All tables were created with the final to match between each other using the state column. This means that you can select all immigrants for New York State and at the same time know which airports you have available in that state, or know what was the average temperature on a particular day for New York.

To build this project we're going to use Spark and Postgres.

Spark shine with largest datasets like US National Immigration, that's why we're going to use Spark Dataframes to build all tables. The other reason is that it's really simple to connect with Postgres.
Spark provides methods to write directly into Postgres without the necessity to create tables, Spark creates tables by himself. If you want to delete the data before insert in a dimension table, all you have to do is specified "overwrite" mode in the write statement, the same for "append" mode.

Postgres will allow us to create the Datawarehouse as simple as possible and if we want to migrate in the future to Amazon Redshift, it provides good synergy due to Amazon Redshift was build upon Postgres 8.0.2.

## Datasets:

List of datasets used in this project:

- I94 Immigration Data: This data came from the US National Tourism and Trade Office,
[this dataset](https://travel.trade.gov/research/reports/i94/historical/2016.html) contains all information about immigrants that travel to the US. In this dataset you will find data like ports of entry, airline, the number of flight, type of visa, date of entry, date until allowed to stay in the US. etc.

- Historical Hourly Weather Data: This dataset contains more than 5 years of hourly weather information with various attributes, such as temperature, humidity, air pressure, etc.   [Dataset link](https://www.kaggle.com/selfishgene/historical-hourly-weather-data)

- U.S. City Demographic Data: This dataset is from OpenDataSoft, provides all information about the total population for cities and states of the US. You can download the entire dataset here: [link](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

- Airport Code Table: Dataset with all airports for each city and state of the US. Link to download: [here](https://datahub.io/core/airport-codes#data)


### Dimension Tables:

    - dim_us_city
    - dim_city_temp
    - dim_airport
    - dim_country

### Fact Table:

    - immigration_us


### Database Model


![img](https://imgur.com/AISwbAs.png)


## Getting Started

To execute this project successfully we have two options
The first one is to execute cell by cell of this notebook. The second option is to execute a series of scripts created to accomplish the same as this notebook but without all analysis made it here.

### Jupyter Notebook option

- This is pretty straightforward, we can accomplish this executing cell by cell of this notebook but first, we need to execute a few files to create a database and tables for this project.
I recommend executing this project following this notebook because this provides wide information about why we choose some clean methods, how we solve null values, columns, pivot, etc.
This will give you more information about how we approached every step of this project.

1. Execute `create_tables.py`: This file creates a database called `imm_dwh` and all tables used in this project.
2. After that, we can execute all cells of this notebook without a problem.

### Python script option

- As we said before, we can accomplish the same goal executing a series of python files, the unique difference is that in the script we don't implement analysis methods like print a Dataframe after every clean process. That's the main difference with the Jupyter notebook option.

1. Execute `create_tables.py`: This file creates a database called `imm_dwh` and all tables used in this project.
2. Execute `etl.py`: This file executes the ETL pipeline to build and write into all tables of the Datawarehouse.

### Aditional Information

- Data Dictionary: We add a file called `data_dictionary.md` (To be watched on Github without problems) that contains a dictionary an explanation about every column of this Datawarehouse.
- SQL Queries: You can find all queries like create tables and select in the file called `sql_queries.py`
- Credentials of AWS: Credentials of AWS are already implemented in `etl.py` file, if we want to migrate this project to the cloud.


## FAQs about the project

* __What is the rationale for the choice of tools and technologies for the project?__

__Apache Spark__

We choose to work with Spark having in mind the scalability that may upcoming from our dataset and have the ability to transform data quickly at scale. For example, Pandas in an amazing library and you can accomplish pretty much all of this code using it but is most suitable for working with data that fits into one single machine. So what can happen if we want to process data for 2016, 2017, 2018, 2019? That's not a good approach for Pandas library. This is when Spark shine. Spark will help us to distribute over the cluster doing all tasks faster and without bottleneck. At the moment we're just running a spark job on-premises but we can deploy this on EC2 and take all advantages of AWS.

Another reason of why we choose Spark is his impressive Machine Learning library MLib. This project was created having in mind to build a structured Datawarehouse suitable for Analysis and Machine Learning. So if the company wants to make use of all this data and apply Machine Learning models, Spark is the best choice.

Cleaning the data for Data Analysis is a crucial process, Spark helps us with this too with the capacity to deploy Dataframes and analyze the data before inserting into the database. This provides data integration in the Datawarehouse.

__PostgreSQL__

Postgres provide an excellent synergy with Spark. You can write a Spark Dataframe into a Postgres database without the necessity to create a table.

Postgres is a relational database, this fits perfectly with the scope of the project and if in the future we want to migrate the entire database into AWS, Amazon Redshift was built based on Postgres 8.0.2, so this provides less work and problems if we have to implement constraints, triggers, stored procedures, etc. It's relevant to say that the syntax of queries is the same too.


* __How often the data should be updated and why?__

Our main dataset immigration has information every day. But it's not crucial to update the data for every new row. We need to have in mind that this dataset can be joined with weather information, city population, airport, and states.

Immigration and weather tables can be updated one time a day, why? because the weather table provides temperature information grouped by day and city. In this way, when we run the ETL, the data from the day before will be processed and calculate the average temperature for every city. At the same time, we can load the data for immigrants.

The other datasets like city population, airport and states can be updated one or two times a year. We need to have in mind that this dataset doesn't change in a long time. The next Census is scheduled to 2020, so the period of an update is relatively long compared to the other datasets.


* __How to approach the problem differently under the following scenarios:__
 * __The data was increased by 100x.__

This project is actually running on a single machine. If the data increase by 100x, first of all, we need to migrate the data to the cloud. A good approach is to use EMR on AWS, the idea is to execute this notebook, save the data in parquet files into an S3 bucket and provide information for BI tools and machine learning models.

One of the benefits of implementing EMR is the pricing, we pay per-instant rate for every second used. This means that we don't have to be running EC2 machines 24hours daily to execute a job 1 hour a day. We just pay for the hour of service used.

Another benefit is his auto-scaling. We can provide one or thousands of compute instances to process the data. We can increase or decrease the number of nodes manually or with auto-scaling. This is a good deal is we are not totally sure about how many nodes we need to process the data. This also means that we spend less time tuning and monitoring the clusters.


![img](https://imgur.com/ZNvwLu1.png)


 * __The data populates a dashboard that must be updated on a daily basis by 7am every day.__

To schedule a job, a good approach is to make use of Apache Airflow. We can accomplish pretty much the same using CRON scheduler but Apache Airflow provides a more robust system. With Apache Airflow we have the ability to monitor, schedule and re-run failed events.

To run a Spark Job using Apache Airflow we can make use of our `etl.py` file. This file is the same process than this notebook but instead, it doesn't made use of the analysis process, the file just read, clean and write data into Postgres.

Using a BashOperator we can create a task and indicate in the bash_command variable the .py file we want to submit.

A good tutorial to accomplish this task can be found here: [link](https://blog.insightdatascience.com/scheduling-spark-jobs-with-airflow-4c66f3144660)

 * __The database needed to be accessed by 100+ people.__

For this purpose, the database needs to migrate to the cloud. A good approach will be make use of Amazon Redshift.

We can create groups for security and control access for the entire organization, e.g design a group for every department of the company. In this way, we don't compromise the information.

Amazon Redshift is based on Postgres 8.0.2. If we want to migrate the data into Redshift, first we may need to make some adjustments like constraints, and SQL syntax. For example, if we have a table that makes use of ON CONFLICT constraint. We have to implement this task using other methods, like select distinct, or filter the data before insert. This is because Postgres 8.0.2 doesn't provide this kind of constraints like Postgres 11.
