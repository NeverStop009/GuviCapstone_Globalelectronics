import pandas as pd
import dask.dataframe as dd
import os
import numpy as np
import time
import mysql.connector
import sqlalchemy


#df = pd.read_csv('Customers.csv',encoding='unicode_escape')
#df = pd.read_csv('Stores.csv',encoding='unicode_escape')
#df = pd.read_csv('Sales.csv',encoding='unicode_escape')
#df = pd.read_csv('Products.csv',encoding='unicode_escape')
df = pd.read_csv('Exchange_Rates.csv',encoding='unicode_escape')


conn = sqlalchemy.create_engine(
'mysql+mysqlconnector://root:''@localhost:3306/global')

df.to_sql(
            name='Exchange_Rates', # database table
            con=conn, # database connection
            index=False, # Don't save index
            if_exists='append',
            chunksize =2000
            )

print(df) 
