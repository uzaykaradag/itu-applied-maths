from pyspark.sql.functions import map_concat, lit, create_map, explode, col
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_utils():
    """
    !!!DO NOT TOUCH!!!
    This function returns spark context object and spark session object.
    These are the entry point into all functionality in Spark.
    :return: SparkContext, SparkSession
    """
    conf = SparkConf().setAppName("Covid"). \
        set("spark.mongodb.input.uri", "mongodb://127.0.0.1"). \
        set("spark.mongodb.output.uri", "mongodb://127.0.0.1"). \
        set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"). \
        set("spark.sql.debug.maxToStringFields", 1000)
    spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    return sc, spark



def generate_covid_info(raw_data_df):
    """
    This function extract data column which is dictionary from raw data and then convert it to new dataframe.
    You should also add location column to data column dictionary. It should be similar to like below.

    +-----------------------------------------------------------------------------------+
    |{date -> 2020-02-24, total_cases -> 1.0, new_cases -> 1.0, location -> Afghanistan}|
    |{date -> 2020-02-25, total_cases -> 1.0, new_cases -> 0.0, location -> Afghanistan}|
    |{date -> 2020-02-26, total_cases -> 1.0, new_cases -> 0.0, location -> Afghanistan}|
    |{date -> 2020-02-27, total_cases -> 1.0, new_cases -> 0.0, location -> Afghanistan}|
    |{date -> 2020-02-28, total_cases -> 1.0, new_cases -> 0.0, location -> Afghanistan}|
    +-----------------------------------------------------------------------------------+
    Hint**: You can use explode, map_concat, create_map and lit functions from spark.

    After this evaluation, you should convert it to dataframe. Actually If our dataset has a specific schema,
    we can do easily. But in the time, data column has been changed and new variables added to dictionary.
    So first of all you should find all key values from the above row maps and then you can use them to generate new
    column for our final dataframe. So the output something like that

    +------------+-----------------------+---------------------+-----------+----------+-----------+
    |total_deaths|total_cases_per_million|new_cases_per_million|total_cases|      date|   location|
    +------------+-----------------------+---------------------+-----------+----------+-----------+
    |      2642.0|                 1549.0|                4.573|    60300.0|2021-05-03|Afghanistan|
    |    122554.0|               3415.529|                5.412|  4578852.0|2021-05-03|     Africa|
    |      2399.0|              45616.791|               13.205|   131276.0|2021-05-03|    Albania|
    |      3280.0|               2798.497|                4.447|   122717.0|2021-05-03|    Algeria|
    |       127.0|             172070.148|              168.252|    13295.0|2021-05-03|    Andorra|
    +------------+-----------------------+---------------------+-----------+----------+-----------+
    Hint**: You might collect data to the driver using action functions. Also to create new column you can use
    col function with getItem function. You should evaluate column list with that approach and then you can
    extract column values from above row maps.

    :param raw_data_df: Dataframe
    :return: Dataframe
    """
    #Your Code Here
    sc, spark = get_spark_utils()
    raw_data_df_exploded =  raw_data_df.select(raw_data_df.location, explode(raw_data_df.data))
    raw_data_df_exploded = raw_data_df_exploded.withColumn("location_map",create_map(lit("location"),col("location"))).drop("location")
    data_map = raw_data_df_exploded.select(map_concat("col", "location_map").alias("data_map"))
    data_map_list = data_map.select('data_map').rdd.flatMap(lambda x: x).collect()
    covid_info = spark.read.json(sc.parallelize(data_map_list)).na.fill('')
    
    return covid_info

def generate_country_info(raw_data_df):
    """
    This function filters non data columns from the raw data.

    :param raw_data_df: Dataframe
    :return: Dataframe
    """
    #Your Code Here
    country_info = raw_data_df.drop("data")

    return country_info


def transform_data(covid_data_rdd, datetime_date=None):
    """
    This function converts your raw to new shape. Your raw data actually contains two important knowledge which are
    Country Info and Covid Info. You will use spark functions to evaluate these information and then generate two
    dataframe belonging to them. For all that you will create a condition according to date. Because for the first
    run you are going to make initial load to mongodb. After that you are going to load your data in daily runs.
    Only for initial load you can generate country info dataframe using generate_country_info function and generate
    covid info dataframe using generate_covid_info function. But It is not necessary to evaluate country info dataframe
    for daily runs. For daily runs also url returns you all data, so you should filter daily data according to datetime
    field.

    Example
    -------
    For Initial Load
    :input:
    +-------------+-------------+---------------------+---------+--------------------+
    |aged_65_older|aged_70_older|cardiovasc_death_rate|continent|                data|
    +-------------+-------------+---------------------+---------+--------------------+
    |        2.581|        1.337|              597.029|     Asia|[{date -> 2020-02...|
    |         null|         null|                 null|     null|[{date -> 2020-02...|
    |       13.188|        8.643|              304.195|   Europe|[{new_tests -> 8....|
    |        6.211|        3.857|              278.364|   Africa|[{date -> 2020-02...|
    |         null|         null|              109.135|   Europe|[{date -> 2020-03...|
    +-------------+-------------+---------------------+---------+--------------------+

    :output:
    Covid Info Dataframe
    +------------+-----------------------+---------------------+-----------+----------+-----------+
    |total_deaths|total_cases_per_million|new_cases_per_million|total_cases|      date|   location|
    +------------+-----------------------+---------------------+-----------+----------+-----------+
    |      2642.0|                 1549.0|                4.573|    60300.0|2020-12-24|Afghanistan|
    |    122554.0|               3415.529|                5.412|  4578852.0|2021-02-12|     Africa|
    |      2399.0|              45616.791|               13.205|   131276.0|2021-04-03|    Albania|
    |      3280.0|               2798.497|                4.447|   122717.0|2021-05-09|    Algeria|
    |       127.0|             172070.148|              168.252|    13295.0|2021-01-03|    Andorra|
    +------------+-----------------------+---------------------+-----------+----------+-----------+

    Country Info Dataframe
    +-------------+-------------+---------------------+---------+-------------------+--------------+
    |aged_65_older|aged_70_older|cardiovasc_death_rate|continent|diabetes_prevalence|gdp_per_capita|
    +-------------+-------------+---------------------+---------+-------------------+--------------+
    |        2.581|        1.337|              597.029|     Asia|               9.59|      1803.987|
    |         null|         null|                 null|     null|               null|          null|
    |       13.188|        8.643|              304.195|   Europe|              10.08|     11803.431|
    |        6.211|        3.857|              278.364|   Africa|               6.73|     13913.839|
    |         null|         null|              109.135|   Europe|               7.97|          null|
    +-------------+-------------+---------------------+---------+-------------------+--------------+

    For Daily Load:
    :input:
    +-------------+-------------+---------------------+---------+--------------------+
    |aged_65_older|aged_70_older|cardiovasc_death_rate|continent|                data|
    +-------------+-------------+---------------------+---------+--------------------+
    |        2.581|        1.337|              597.029|     Asia|[{date -> 2020-02...|
    |         null|         null|                 null|     null|[{date -> 2020-02...|
    |       13.188|        8.643|              304.195|   Europe|[{new_tests -> 8....|
    |        6.211|        3.857|              278.364|   Africa|[{date -> 2020-02...|
    |         null|         null|              109.135|   Europe|[{date -> 2020-03...|
    +-------------+-------------+---------------------+---------+--------------------+

    :output:
    Covid Info Dataframe
    +------------+-----------------------+---------------------+-----------+----------+-----------+
    |total_deaths|total_cases_per_million|new_cases_per_million|total_cases|      date|   location|
    +------------+-----------------------+---------------------+-----------+----------+-----------+
    |      2642.0|                 1549.0|                4.573|    60300.0|2021-05-03|Afghanistan|
    |    122554.0|               3415.529|                5.412|  4578852.0|2021-05-03|     Africa|
    |      2399.0|              45616.791|               13.205|   131276.0|2021-05-03|    Albania|
    |      3280.0|               2798.497|                4.447|   122717.0|2021-05-03|    Algeria|
    |       127.0|             172070.148|              168.252|    13295.0|2021-05-03|    Andorra|
    +------------+-----------------------+---------------------+-----------+----------+-----------+

    :param covid_data_rdd: RDD
    :param datetime_date: Datetime
    :return: Dataframe, Dataframe
    """
    
    #Your Code Here
    covid_info = generate_covid_info(covid_data_rdd)
    country_info = generate_country_info(covid_data_rdd)

    if(datetime_date == None):
        return covid_info, country_info
    else:
        date_time = datetime_date.strftime("%Y-%m-%d")
        covid_info_on_date = covid_info.where(covid_info.date == date_time)
        return covid_info_on_date, country_info        
        
