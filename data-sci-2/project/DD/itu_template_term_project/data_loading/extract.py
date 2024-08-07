import requests
import json


def extract_covid_data():
    """
    In this function, you should receive data with using native Python from below url.
    Data is in json format and you should append every record to a list. This list will
    be your raw data and you will use it in initial load to mongodb and for daily load
    to mongoDb.

    https://covid.ourworldindata.org/data/owid-covid-data.json

    Example
    -------
    :input:

    json_data = {"TRY":  {"continent": "Asia", "location": "Turkey",
    "data": [{"case": 50, "date": "03.03.2021"}, {"case": 60, "date": "03.04.2021"}]}

    :output:

    list_data = [{"continent": "Asia", "location": "Turkey",
    "data": [{"case": 50, "date": "03.03.2021"}, {"case": 60, "date": "03.04.2021"}]

    Hint*: Population column's value is received as String. If you convert population value to Int, It can be nice.

    :return: list of covid data according to location
    """
    
    #Your Code Here
    url = 'https://covid.ourworldindata.org/data/owid-covid-data.json'

    response = requests.get(url)
    json_data = json.loads(response.text)

    raw_data = []
    for entry in json_data.values():
        raw_data.append(entry)

    return raw_data
