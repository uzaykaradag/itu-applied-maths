from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import date_format, datediff, to_date, lit, to_timestamp, col, max, min, mean, stddev, count, hour
import matplotlib.pyplot as plt

conf = SparkConf().setAppName("Homework4")
spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()
sc = spark.sparkContext


def find_statistical_information(df):
    """
    Retrieve statistical information (You can use aggregate functions e.g max, average of columns)
    :param df: Dataframe
    """
    # Select only the columns of type double
    columns = [c.name for c in df.schema.fields if isinstance(c.dataType, DoubleType)]

    print("""
    :'######::'########::::'###::::'########:'####::'######::'########:'####::'######:::'######::
    '##... ##:... ##..::::'## ##:::... ##..::. ##::'##... ##:... ##..::. ##::'##... ##:'##... ##:
    ##:::..::::: ##:::::'##:. ##::::: ##::::: ##:: ##:::..::::: ##::::: ##:: ##:::..:: ##:::..::
    . ######::::: ##::::'##:::. ##:::: ##::::: ##::. ######::::: ##::::: ##:: ##:::::::. ######::
    :..... ##:::: ##:::: #########:::: ##::::: ##:::..... ##:::: ##::::: ##:: ##::::::::..... ##:
    '##::: ##:::: ##:::: ##.... ##:::: ##::::: ##::'##::: ##:::: ##::::: ##:: ##::: ##:'##::: ##:
    . ######::::: ##:::: ##:::: ##:::: ##::::'####:. ######::::: ##::::'####:. ######::. ######::
    :......::::::..:::::..:::::..:::::..:::::....:::......::::::..:::::....:::......::::......:::""")

    # Calculate the maximum, minimum, and average values for each column
    max_values = df.select([max(c).alias(c) for c in columns]).toPandas()
    print("MAX VALUES")
    print("-------------------------------------------------------------")
    print(max_values)
    print("-------------------------------------------------------------", "\n\n\n\n")

    min_values = df.select([min(c).alias(c) for c in columns]).toPandas()
    print("MIN VALUES")
    print("-------------------------------------------------------------")
    print(min_values)
    print("-------------------------------------------------------------", "\n\n\n\n")


    avg_values = df.select([mean(c).alias(c) for c in columns]).toPandas()
    print("AVG VALUES")
    print("-------------------------------------------------------------")
    print(avg_values)
    print("-------------------------------------------------------------", "\n\n\n\n")




def join_look_up_with_cities(df):
    """
    You can use this function to join your lookup table and raw datasets.
    :param df: Dataframe
    :return: Dataframe
    """
    tzl = spark.read.csv("./data/taxi_zone_lookup.csv", header=True)
    tzlpu = tzl.withColumnRenamed("Zone", "PUZ")
    dfj = df.join(tzlpu, df.PULocationID == tzl.LocationID, "left")
    tzl = tzl.withColumnRenamed("Zone", "DOZ")
    dfj = dfj.alias("joined").join(tzl.alias("tzl"), df.DOLocationID == F.col("tzl.LocationID"), "left")
    return dfj




def get_most_expensive_route(df):
    """
    What is the most expensive route for each datasets?
    :param df: Dataframe
    """
    df=join_look_up_with_cities(df)
    print(""" 
    '########:'##::::'##:'########::'########:'##::: ##::'######::'####:'##::::'##:'########::::'########:::'#######::'##::::'##:'########:'########::'######::
    ##.....::. ##::'##:: ##.... ##: ##.....:: ###:: ##:'##... ##:. ##:: ##:::: ##: ##.....::::: ##.... ##:'##.... ##: ##:::: ##:... ##..:: ##.....::'##... ##:
    ##::::::::. ##'##::: ##:::: ##: ##::::::: ####: ##: ##:::..::: ##:: ##:::: ##: ##:::::::::: ##:::: ##: ##:::: ##: ##:::: ##:::: ##:::: ##::::::: ##:::..::
    ######:::::. ###:::: ########:: ######::: ## ## ##:. ######::: ##:: ##:::: ##: ######:::::: ########:: ##:::: ##: ##:::: ##:::: ##:::: ######:::. ######::
    ##...:::::: ## ##::: ##.....::: ##...:::: ##. ####::..... ##:: ##::. ##:: ##:: ##...::::::: ##.. ##::: ##:::: ##: ##:::: ##:::: ##:::: ##...:::::..... ##:
    ##:::::::: ##:. ##:: ##:::::::: ##::::::: ##:. ###:'##::: ##:: ##:::. ## ##::: ##:::::::::: ##::. ##:: ##:::: ##: ##:::: ##:::: ##:::: ##:::::::'##::: ##:
    ########: ##:::. ##: ##:::::::: ########: ##::. ##:. ######::'####:::. ###:::: ########:::: ##:::. ##:. #######::. #######::::: ##:::: ########:. ######::
    ........::..:::::..::..:::::::::........::..::::..:::......:::....:::::...:::::........:::::..:::::..:::.......::::.......::::::..:::::........:::......:::
    """)

    tdf = df.groupBy(df['PUZ'], df['DOZ']).agg({"fare_amount": "sum"})
    tdf = tdf.withColumnRenamed("sum(fare_amount)", "fare_amount_sum")

    mtdf = tdf.groupBy(df['PUZ'], df['DOZ']).max("fare_amount_sum")
    mtdf = mtdf.withColumnRenamed("max(fare_amount_sum)", "fare_amount_sum_max")
    mtdf = mtdf.sort(mtdf.fare_amount_sum_max.desc())

    mtdf.show(1)


def get_busiest_taxi_station(df):
    """
    What is the busiest Taxi station for each datasets?
    :param df: Dataframe
    """
    print("""
    '########::'##::::'##::'######::'####:'########::'######::'########:::::'######::'########::::'###::::'########:'####::'#######::'##::: ##:
    ##.... ##: ##:::: ##:'##... ##:. ##:: ##.....::'##... ##:... ##..:::::'##... ##:... ##..::::'## ##:::... ##..::. ##::'##.... ##: ###:: ##:
    ##:::: ##: ##:::: ##: ##:::..::: ##:: ##::::::: ##:::..::::: ##::::::: ##:::..::::: ##:::::'##:. ##::::: ##::::: ##:: ##:::: ##: ####: ##:
    ########:: ##:::: ##:. ######::: ##:: ######:::. ######::::: ##:::::::. ######::::: ##::::'##:::. ##:::: ##::::: ##:: ##:::: ##: ## ## ##:
    ##.... ##: ##:::: ##::..... ##:: ##:: ##...:::::..... ##:::: ##::::::::..... ##:::: ##:::: #########:::: ##::::: ##:: ##:::: ##: ##. ####:
    ##:::: ##: ##:::: ##:'##::: ##:: ##:: ##:::::::'##::: ##:::: ##:::::::'##::: ##:::: ##:::: ##.... ##:::: ##::::: ##:: ##:::: ##: ##:. ###:
    ########::. #######::. ######::'####: ########:. ######::::: ##:::::::. ######::::: ##:::: ##:::: ##:::: ##::::'####:. #######:: ##::. ##:
    ........::::.......::::......:::....::........:::......::::::..:::::::::......::::::..:::::..:::::..:::::..:::::....:::.......:::..::::..::
     """)
    df = join_look_up_with_cities(df)
    cdfpu = df.groupBy('PUZ').count()
    cdfdo = df.groupBy('DOZ').count()
    cdfdo = cdfdo.withColumnRenamed("count", "count_2")
    cdf = cdfpu.join(cdfdo, cdfpu.PUZ == cdfdo.DOZ, "inner")
    cdf1 = cdf.withColumn("sum", F.col("count")+F.col("count_2"))
    cdf1 = cdf1.drop("count", "count_2")
    cdf1.show(5)


def get_top_5_busiest_area(df):
    """
    What is the top 5 busiest Area for each datasets?
    :param df: Dataframe
    """
    print(""" 
    '########::'##::::'##::'######::'####:'########::'######::'########:::::::'###::::'########::'########::::'###:::::'######::
    ##.... ##: ##:::: ##:'##... ##:. ##:: ##.....::'##... ##:... ##..:::::::'## ##::: ##.... ##: ##.....::::'## ##:::'##... ##:
    ##:::: ##: ##:::: ##: ##:::..::: ##:: ##::::::: ##:::..::::: ##::::::::'##:. ##:: ##:::: ##: ##::::::::'##:. ##:: ##:::..::
    ########:: ##:::: ##:. ######::: ##:: ######:::. ######::::: ##:::::::'##:::. ##: ########:: ######:::'##:::. ##:. ######::
    ##.... ##: ##:::: ##::..... ##:: ##:: ##...:::::..... ##:::: ##::::::: #########: ##.. ##::: ##...:::: #########::..... ##:
    ##:::: ##: ##:::: ##:'##::: ##:: ##:: ##:::::::'##::: ##:::: ##::::::: ##.... ##: ##::. ##:: ##::::::: ##.... ##:'##::: ##:
    ########::. #######::. ######::'####: ########:. ######::::: ##::::::: ##:::: ##: ##:::. ##: ########: ##:::: ##:. ######::
    ........::::.......::::......:::....::........:::......::::::..::::::::..:::::..::..:::::..::........::..:::::..:::......:::
    """)
    df = join_look_up_with_cities(df)
    df.groupby("PUZ").agg(F.count("*").alias("trip_count")).sort(col("trip_count").desc()).show(5)

#
def get_longest_trips(df):
    """
    What is the longest trip in each dataset?
    :param df: Dataframe
    """
    print("""'
    ##::::::::'#######::'##::: ##::'######:::'########::'######::'########::::'########:'########::'####:'########:::'######::
    ##:::::::'##.... ##: ###:: ##:'##... ##:: ##.....::'##... ##:... ##..:::::... ##..:: ##.... ##:. ##:: ##.... ##:'##... ##:
    ##::::::: ##:::: ##: ####: ##: ##:::..::: ##::::::: ##:::..::::: ##:::::::::: ##:::: ##:::: ##:: ##:: ##:::: ##: ##:::..::
    ##::::::: ##:::: ##: ## ## ##: ##::'####: ######:::. ######::::: ##:::::::::: ##:::: ########::: ##:: ########::. ######::
    ##::::::: ##:::: ##: ##. ####: ##::: ##:: ##...:::::..... ##:::: ##:::::::::: ##:::: ##.. ##:::: ##:: ##.....::::..... ##:
    ##::::::: ##:::: ##: ##:. ###: ##::: ##:: ##:::::::'##::: ##:::: ##:::::::::: ##:::: ##::. ##::: ##:: ##::::::::'##::: ##:
    ########:. #######:: ##::. ##:. ######::: ########:. ######::::: ##:::::::::: ##:::: ##:::. ##:'####: ##::::::::. ######::
    ........:::.......:::..::::..:::......::::........:::......::::::..:::::::::::..:::::..:::::..::....::..::::::::::......::: """)
    try:
        df = df.withColumn("pickup_time", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
        df = df.withColumn("dropoff_time", date_format(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
    except:
        df = df.withColumn("pickup_time", date_format(col("lpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
        df = df.withColumn("dropoff_time", date_format(col("lpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))

    df = df.withColumn("journey_time_mins", datediff(col("dropoff_time"), col("pickup_time")) * 1440)

    df.sort(col("journey_time_mins").desc()).show(1)

def get_crowded_places_per_hour(df):
    """
    Find the crowded Pickup and Drop-off zones for each hour.
    :param df: Dataframe
    """
    print("""
     :'######::'########:::'#######::'##:::::'##:'########::'########:'########:::::'########::'##::::::::::'###:::::'######::'########::'######::
    '##... ##: ##.... ##:'##.... ##: ##:'##: ##: ##.... ##: ##.....:: ##.... ##:::: ##.... ##: ##:::::::::'## ##:::'##... ##: ##.....::'##... ##:
    ##:::..:: ##:::: ##: ##:::: ##: ##: ##: ##: ##:::: ##: ##::::::: ##:::: ##:::: ##:::: ##: ##::::::::'##:. ##:: ##:::..:: ##::::::: ##:::..::
    ##::::::: ########:: ##:::: ##: ##: ##: ##: ##:::: ##: ######::: ##:::: ##:::: ########:: ##:::::::'##:::. ##: ##::::::: ######:::. ######::
    ##::::::: ##.. ##::: ##:::: ##: ##: ##: ##: ##:::: ##: ##...:::: ##:::: ##:::: ##.....::: ##::::::: #########: ##::::::: ##...:::::..... ##:
    ##::: ##: ##::. ##:: ##:::: ##: ##: ##: ##: ##:::: ##: ##::::::: ##:::: ##:::: ##:::::::: ##::::::: ##.... ##: ##::: ##: ##:::::::'##::: ##:
    . ######:: ##:::. ##:. #######::. ###. ###:: ########:: ########: ########::::: ##:::::::: ########: ##:::: ##:. ######:: ########:. ######::
    :......:::..:::::..:::.......::::...::...:::........:::........::........::::::..:::::::::........::..:::::..:::......:::........:::......:::
    """)
    try:
        df = df.withColumn("hour", hour(col("tpep_pickup_datetime")))
    except:
        df = df.withColumn("hour", hour(col("lpep_pickup_datetime")))

    df = df.groupBy(col("PULocationID"), col("DOLocationID"), col("hour")).count()

    return df.sort(col("count").desc())


def get_busiest_hours(df):
    """
    Find the Pickup and Drop-off count for each hour. After that draw two lineplot graphs for Pickup and Drop-off.
    This function returns busiest hour that you will use it in draw_busiest_hours_graph to draw lineplot graph.
    Hint**: You can use window functions.
    :param df: Dataframe
    :return: Dataframe
    """
    try:
        df = df.withColumn("ph", F.date_format(F.col("tpep_pickup_datetime"), "H"))
        df = df.withColumn("dh", F.date_format(F.col("tpep_dropoff_datetime"), "H"))
    except:
        df = df.withColumn("ph", F.date_format(F.col("lpep_pickup_datetime"), "H"))
        df = df.withColumn("dh", F.date_format(F.col("lpep_dropoff_datetime"), "H"))

    pudf = df.groupBy("ph").count()
    dodf = df.groupBy("dh").count()
    dodf = dodf.withColumnRenamed("count", "count_2")

    hcdf = pudf.join(dodf, pudf.ph == dodf.dh, "inner")
    hcdf_1 = hcdf.withColumn("sum", F.col("count") + F.col("count_2"))
    hcdf_1 = hcdf_1.drop("count", "count_2", "dh")
    hcdf_1 = hcdf_1.withColumnRenamed("ph", "hour")
    
    return hcdf_1


def draw_busiest_hours_graph(df):
    """
    You will use get_busiest_hours' result here. With this dataframe you should draw hour to count lineplot.
    Hint**: You can convert Spark Dataframe to Pandas Dataframe here.
    :param df: Dataframe
    """
    print("""
    '########::'##::::'##::'######::'####:'########::'######::'########::::'##::::'##::'#######::'##::::'##:'########:::'######::::::'######:::'########:::::'###::::'########::'##::::'##:
    ##.... ##: ##:::: ##:'##... ##:. ##:: ##.....::'##... ##:... ##..::::: ##:::: ##:'##.... ##: ##:::: ##: ##.... ##:'##... ##::::'##... ##:: ##.... ##:::'## ##::: ##.... ##: ##:::: ##:
    ##:::: ##: ##:::: ##: ##:::..::: ##:: ##::::::: ##:::..::::: ##::::::: ##:::: ##: ##:::: ##: ##:::: ##: ##:::: ##: ##:::..::::: ##:::..::: ##:::: ##::'##:. ##:: ##:::: ##: ##:::: ##:
    ########:: ##:::: ##:. ######::: ##:: ######:::. ######::::: ##::::::: #########: ##:::: ##: ##:::: ##: ########::. ######::::: ##::'####: ########::'##:::. ##: ########:: #########:
    ##.... ##: ##:::: ##::..... ##:: ##:: ##...:::::..... ##:::: ##::::::: ##.... ##: ##:::: ##: ##:::: ##: ##.. ##::::..... ##:::: ##::: ##:: ##.. ##::: #########: ##.....::: ##.... ##:
    ##:::: ##: ##:::: ##:'##::: ##:: ##:: ##:::::::'##::: ##:::: ##::::::: ##:::: ##: ##:::: ##: ##:::: ##: ##::. ##::'##::: ##:::: ##::: ##:: ##::. ##:: ##.... ##: ##:::::::: ##:::: ##:
    ########::. #######::. ######::'####: ########:. ######::::: ##::::::: ##:::: ##:. #######::. #######:: ##:::. ##:. ######:::::. ######::: ##:::. ##: ##:::: ##: ##:::::::: ##:::: ##:
    ........::::.......::::......:::....::........:::......::::::..::::::::..:::::..:::.......::::.......:::..:::::..:::......:::::::......::::..:::::..::..:::::..::..:::::::::..:::::..::
    """)
    hdf = get_busiest_hours(df)
    hdf_pandas = hdf.toPandas()
    hdf_pandas.plot()
    plt.show()


def get_tip_correlation(df):
    """
    (BONUS) What do other columns affect the tip column? (HINT: Correlation of tip amount to other columns)
    :param df: Dataframe
    """
    print("""
    '########:'####:'########::::::'######:::'#######::'########::'########::'########:'##::::::::::'###::::'########:'####::'#######::'##::: ##:
    ... ##..::. ##:: ##.... ##::::'##... ##:'##.... ##: ##.... ##: ##.... ##: ##.....:: ##:::::::::'## ##:::... ##..::. ##::'##.... ##: ###:: ##:
    ::: ##::::: ##:: ##:::: ##:::: ##:::..:: ##:::: ##: ##:::: ##: ##:::: ##: ##::::::: ##::::::::'##:. ##::::: ##::::: ##:: ##:::: ##: ####: ##:
    ::: ##::::: ##:: ########::::: ##::::::: ##:::: ##: ########:: ########:: ######::: ##:::::::'##:::. ##:::: ##::::: ##:: ##:::: ##: ## ## ##:
    ::: ##::::: ##:: ##.....:::::: ##::::::: ##:::: ##: ##.. ##::: ##.. ##::: ##...:::: ##::::::: #########:::: ##::::: ##:: ##:::: ##: ##. ####:
    ::: ##::::: ##:: ##::::::::::: ##::: ##: ##:::: ##: ##::. ##:: ##::. ##:: ##::::::: ##::::::: ##.... ##:::: ##::::: ##:: ##:::: ##: ##:. ###:
    ::: ##::::'####: ##:::::::::::. ######::. #######:: ##:::. ##: ##:::. ##: ########: ########: ##:::: ##:::: ##::::'####:. #######:: ##::. ##:
    :::..:::::....::..:::::::::::::......::::.......:::..:::::..::..:::::..::........::........::..:::::..:::::..:::::....:::.......:::..::::..::
    """)
    dfp = df.toPandas()
    return (dict(dfp.corr()['tip_amount'].sort_values()))

    
if __name__ == '__main__':
    """
    In this main function first of all you should read dataset separately. Then you can call the functions in orderly.
    """
    ydf = spark.read.parquet("./data/yellow_tripdata_2021-03.parquet")
    gdf = spark.read.parquet("./data/green_tripdata_2021-03.parquet")

    for i, df in enumerate([ydf, gdf]):
        print("For the {}. Dataset\n\n\n\n".format(i))

        find_statistical_information(df)
        get_most_expensive_route(df)
        get_busiest_taxi_station(df)
        get_top_5_busiest_area(df)
        get_longest_trips(df)
        get_crowded_places_per_hour(df).show(10)
        draw_busiest_hours_graph(df)
        print(get_tip_correlation(df))
