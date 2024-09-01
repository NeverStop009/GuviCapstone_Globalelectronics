

#CustomerKey,Gender,Name,City,State Code,State,Zip Code,Country,Continent,Birthday

#C:\Users\AMD Ryzen\AppData\Local\Programs\Python\Python310\guvi_project2\Guvi_2_project

#..\\data_folder\\data.csv

import pandas as pd
import dask.dataframe as dd
import os
import numpy as np
import time
import mysql.connector
import sqlalchemy


#df = pd.read_csv('Customers.csv',encoding='unicode_escape')
df_cust = pd.read_csv('Customers.csv',encoding='unicode_escape')
df_sales = pd.read_csv('Sales.csv')
df_products = pd.read_csv('Products.csv')
df_stores= pd.read_csv('Stores.csv')

new_df = pd.merge(df_sales ,df_cust,on='CustomerKey')
df_new=pd.merge(new_df,df_products)
df_new=pd.merge(df_new,df_stores)


conn = sqlalchemy.create_engine(
'mysql+mysqlconnector://root:''@localhost:3306/globalelectronics')

df_new.to_sql(
            name='products', # database table
            con=conn, # database connection
            index=False, # Don't save index
            if_exists='append',
            chunksize =2000
            )

#prod_df = pd.merge(df_products ,df_sales,on='OrderNumber')
print(df_new.info())
print(df_new)
print(df_new.info())
print(df_new.head)
#print(prod_df.info())
#print(prod_df)
