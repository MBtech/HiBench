import numpy as np
import pandas as pd 
from joblib import Parallel, delayed
import math
import sys 
import boto3
import json

house_size = range(50, 1000, 10)
house_loc = range(0, 10000)
house_rooms =range(1, 12)
house_bathrooms = range(1, 12)
house_years = range(0, 100)

np.random.seed(10)
crime_ratings = {loc: np.random.choice(range(1, 11)) for loc in house_loc}
salary = {loc: np.random.choice(range(20, 100, 10)) for loc in house_loc}

def house_data(start, stop, loc_data = None):
    if loc_data is None:
        data = pd.DataFrame(columns=["id", "size", "loc", "rooms", "bathrooms", "year", "price"])
        for i in range(start, stop):
            size = np.random.choice(house_size)
            loc = np.random.choice(house_loc)
            rooms = np.random.choice(house_rooms)
            bathrooms = np.random.choice(house_bathrooms)
            year = np.random.choice(house_years) 
            price = (1000*size) + (5000*salary[loc]) + (5000*crime_ratings[loc]) + (1000*rooms) + (500*bathrooms) +  (house_years[-1] - year) *10000
            data.loc[len(data)] = [i, size, loc, rooms, bathrooms, year, price]
        
        data.index.name = 'index'
        data.to_csv('sample_data/housing_data_' +str(start)+'.csv')
        s3 = boto3.resource('s3')
    
        s3.meta.client.upload_file('sample_data/housing_data_' +str(start)+'.csv', configs["bucket.name"], 'current-data/housing_data_' +str(start)+'.csv')
    else:
        data = pd.DataFrame(columns=["id", "size", "loc", "rooms", "bathrooms", "year", "price", "crime", "salary"])
        for i in range(start, stop):
            size = np.random.choice(house_size)
            loc = np.random.choice(house_loc)
            rooms = np.random.choice(house_rooms)
            bathrooms = np.random.choice(house_bathrooms)
            year = np.random.choice(house_years) 
            price = (1000*size) + (5000*salary[loc]) + (5000*crime_ratings[loc]) + (1000*rooms) + (500*bathrooms) +  (house_years[-1] - year) *10000
            data.loc[len(data)] = [i, size, loc, rooms, bathrooms, year, price, loc_data.iloc[loc]["crime"],loc_data.iloc[loc]["salary"] ]
        
        data.index.name = 'index'
        data.to_csv('sample_data/data.csv')
        s3 = boto3.resource('s3')
    
        s3.meta.client.upload_file('sample_data/data.csv', configs["bucket.name"], 'all-data/data.csv')

    return data

def loc_data_generator():
    data = pd.DataFrame(columns=["loc", "crime", "salary"])
    print(house_loc)
    for loc in house_loc:
        crime = crime_ratings[loc]
        s = salary[loc]
        data.loc[len(data)] = [loc, crime, s]

    data.index.name = 'index'
    data.to_csv('sample_data/loc_data.csv')
    s3 = boto3.resource('s3')
    
    s3.meta.client.upload_file('sample_data/loc_data.csv', configs["bucket.name"], 'loc_data.csv')
    return data

def init_data_generator():
    print("Generating Init Data")
    loc_data = loc_data_generator()
    house_data(0, 10000, loc_data)
    

def housing_data_generator(start=0, n_points=1000000, n_jobs=4):

    # step = int(math.ceil(n_points/n_jobs))

    # Parallel(n_jobs=n_jobs)(delayed(house_data)(i, i+step) for i in range(start, n_points, step))

    from pyspark.mllib.random import RandomRDDs
    from pyspark.sql import SparkSession
    from pyspark.sql import DataFrame
    from pyspark.sql import SQLContext, Row
    
    import math
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .getOrCreate()

    columns=["id", "size", "loc", "rooms", "bathrooms", "year", "price"]

    u = RandomRDDs.uniformRDD(scSpark, n_points, 2).map(lambda x: math.ceil(house_size[0] + (house_size[-1] - house_size[0]) * x)).zipWithIndex()
    u = scSpark.createDataFrame(u, ["size", "id"])

    for col in columns[2:]:
        v = RandomRDDs.uniformRDD(scSpark, n_points, 2).map(lambda x: math.ceil(house_size[0] + (house_size[-1] - house_size[0]) * x)).zipWithIndex()
        v = scSpark.createDataFrame(v, [col, "id"])
        u = u.join(v, "id").select("*")

        u.show()

configs = json.load(open('config.json'))
if sys.argv[1] == "init":
    init_data_generator()
else:
    housing_data_generator(0, 100000,4)
# loc_data_generator()