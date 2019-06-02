

dim_us_city_create = """
CREATE TABLE IF NOT EXISTS dim_us_city
(
    city VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    male_population INTEGER,
    female_population INTEGER,
    total_population INTEGER,
    state_prefix VARCHAR NOT NULL
)   DISTSTYLE ALL

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
