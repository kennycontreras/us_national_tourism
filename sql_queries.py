# DROP TABLES

dim_airport_drop = "DROP TABLE IF EXISTS dim_airport"
dim_country_drop = "DROP TABLE IF EXISTS dim_us_country"
dim_city_drop = "DROP TABLE IF EXISTS dim_us_city"
dim_weather_drop = "DROP TABLE IF EXISTS dim_us_weather"
fact_imm_drop = "DROP TABLE IF EXISTS immigration_us"

# CREATE TABLES

dim_airport_create = """
    CREATE TABLE IF NOT EXISTS public.dim_airport
    (
        id_airport text COLLATE pg_catalog."default" PRIMARY KEY,
        type text COLLATE pg_catalog."default",
        name text COLLATE pg_catalog."default",
        country text COLLATE pg_catalog."default",
        state text COLLATE pg_catalog."default",
        city text COLLATE pg_catalog."default"
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

    ALTER TABLE public.dim_airport
        OWNER to student;
"""

dim_country_create = """
    CREATE TABLE IF NOT EXISTS public.dim_country
    (
        id_country text COLLATE pg_catalog."default" PRIMARY KEY,
        country text COLLATE pg_catalog."default"
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

    ALTER TABLE public.dim_country
        OWNER to student;
"""

dim_us_city_create = """
    CREATE TABLE public.dim_us_city
    (
        id_city SERIAL PRIMARY KEY,
        city text COLLATE pg_catalog."default",
        state text COLLATE pg_catalog."default",
        male_population text COLLATE pg_catalog."default" NOT NULL,
        female_population text COLLATE pg_catalog."default" NOT NULL,
        total_population text COLLATE pg_catalog."default",
        state_prefix text COLLATE pg_catalog."default"
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

    ALTER TABLE public.dim_us_city
        OWNER to student;
"""

dim_weather_create = """
    CREATE TABLE public.dim_us_weather
    (
        id_weather SERIAL PRIMARY KEY,
        datetime date,
        city text COLLATE pg_catalog."default",
        temp text COLLATE pg_catalog."default",
        state text COLLATE pg_catalog."default"
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

    ALTER TABLE public.dim_us_weather
        OWNER to student;
"""

fact_imm_create = """
    CREATE TABLE public.immigration_us
    (
        id integer PRIMARY KEY,
        year integer,
        month integer,
        citizen integer,
        resident integer,
        port_entry text COLLATE pg_catalog."default",
        mode_entry integer,
        arrival_date date,
        dep_date date,
        dateadd_to date,
        state_addr text COLLATE pg_catalog."default",
        birth_year integer,
        age integer,
        gender text COLLATE pg_catalog."default",
        visa_code integer,
        visa_type text COLLATE pg_catalog."default",
        airline text COLLATE pg_catalog."default"
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

    ALTER TABLE public.immigration_us
        OWNER to student;
"""


# DATA QUALITY QUERIES


select_imm_count = """
    SELECT COUNT(1) FROM IMMIGRATION_US;
"""

select_arrival_date = """
    SELECT
    SUM(CASE WHEN ARRIVAL_DATE IS NULL THEN 1 ELSE 0 END) NULL_VALUES,
    SUM(CASE WHEN ARRIVAL_DATE IS NOT NULL THEN 1 ELSE 0 END) NOT_NULL_VALUES
    FROM IMMIGRATION_US;
"""

select_year_imm = """
    SELECT DISTINCT YEAR
    FROM IMMIGRATION_US
    ORDER BY YEAR;
"""

select_year_weather = """
    SELECT DISTINCT CAST(EXTRACT(YEAR FROM datetime) AS INTEGER) AS YEAR
    FROM DIM_US_WEATHER
    ORDER BY YEAR;
"""

# query list

drop_table_queries = [dim_airport_drop, dim_country_drop,
                      dim_city_drop, dim_weather_drop, fact_imm_drop]
create_table_queries = [dim_airport_create, dim_country_create,
                        dim_us_city_create, dim_weather_create, fact_imm_create]
