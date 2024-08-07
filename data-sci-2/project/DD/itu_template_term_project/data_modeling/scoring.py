import json
import pickle

import pandas as pd
from pymongo import MongoClient
from sklearn import preprocessing


def _connect_mongo(host, port, db):
    """
    This function returns mongodb connection
    :param host:string
    :param port: string
    :param db: string
    :return: connection
    """
    conn = MongoClient(host, port)
    return conn[db]


def create_covid_dataframe(df):
    """
    You can use this function to create covid dataframe after reading from mongodb. Because some days don't contains
    all columns from column_list.txt file. So If your model can get error because of absent columns.
    :param df: Pandas dataframe which is initialized from mongodb and con have absent columns
    :return: new dataframe with all columns.
    """
    with open('model/column_list.txt', 'r') as file:
        columns = [column.strip() for column in file.readlines()]
    empty_df = pd.DataFrame(columns=columns)
    return df.reindex(columns=empty_df.columns)


def read_mongo(db, collection, query={}, host='localhost', port=27017, no_id=True):
    """
    This function read data from mongodb collection. Also you can give query parameter which benefits to query filtering
    data. Mongodb collection's reside as json. When you read data from mongodb to python, It will be converted to
    dictionary. So you should convert it from dictionary to pandas dataframe. Also If you want, you can add a condition 
    to delete "_id" column. 
    :param db: Database name in mongodb
    :param collection: Collection name in mongodb database
    :param query: Mongodb query to filter in dictionary type
    :param host: mongodb server ip/host. Default "localhost"
    :param port: mongodb server port. Default "27017"
    :param no_id: mongodb returns data with "_id" column. If you think that It is not necessary, you can set True.
    Default true.
    :return: Pandas Dataframe
    """
    db = _connect_mongo(host=host, port=port, db=db)
    #Your Code Here

def write_mongo(db, collection, score, host='localhost', port=27017):
    """
    This function write pandas dataframe to mongodb.
    :param db: Database name in mongodb
    :param collection: Collection name in mongodb database
    :param score: Pandas dataframe which contains scores and input variables
    :param host: mongodb server ip/host. Default "localhost"
    :param port: mongodb server port. Default "27017"
    """

    db = _connect_mongo(host=host, port=port, db=db)
    #Your Code Here


def feature_formatting(df_all):
    """
    In this function you should make some changes on columns. First, you should change date column type to datetime.
    After that, non-categorical columns types can be updated like converting float.

    Example
    -------
    When you convert dictionary to pandas dataframe, Column types can be like below
    22  stringency_index                 171 non-null    object
    23  total_cases                      193 non-null    object
    24  date                             194 non-null    object

    Your new type should be similar below.

    22  stringency_index                 171 non-null    float64
    23  total_cases                      193 non-null    float64
    24  date                             194 non-null    datetime64[ns]

    :param df_all: Dataframe
    :return:
    """

    #Your Code Here


def feature_generation(df_feature_formatted):
    """
    In this function you will generate your new columns which are evaluated in modeling phase. Which means that if
    you need new columns for your model. You can generate them here.

    Hint**: If you generate avg, sum, etc columns for a couple of weeks or days for your model, you should read required
    date range. For example, If we have average of last 3 day, you should read 27, 28, 29th days from mongodb to score
    30th day. Be careful about that.

    :param df_feature_formatted: Pandas Dataframe
    :return:
    """
    #Your Code Here

def label_encoding(df):
    """
    This function converts char columns to numeric values. This can be helpful to run models.

    Example
    -------
    continent     location       date
      Asia       Afghanistan     2021-05-01
      Asia       Afghanistan     2021-05-02
      Asia       Afghanistan     2021-05-03
      Asia       Afghanistan     2021-05-04


   continent_num  location_num   date
           2             0       2021-05-01
           2             0       2021-05-02
           2             0       2021-05-03

    :param df: Pandas dataframe
    :return: Labeled Pandas Dataframe
    """
    #Your Code Here


def get_scores(df):
    """
    This function will generates your scores. You will read your model from pickle file and them apply it onto the
    dataframe to get next day covid case number. It will return new dataframe with score column.
    :param df: Input data for model as dataframe
    :return: Output data with scores as dataframe
    """

    #Your Code Here


if __name__ == '__main__':
    """
    This your main function and flow will be here. 
    1. Read country info and covid data from mongodb. These data should be loaded previously with ETL.
    2. Merge these two dataset
    3. Apply Feature formatting
    4. Apply Feature generation
    5. Apply Label Encoding
    6. Find Scores
    7. Write them to Mongodb
    """
    # Your Code Here
