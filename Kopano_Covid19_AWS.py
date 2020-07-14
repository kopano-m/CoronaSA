#!/usr/bin/env python
# coding: utf-8

# In[10]:


import numpy as np
import pandas as pd

df_C = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv")
df_D = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv")
recovered_df = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv")
df_tests = pd.read_csv("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_testing.csv")
df_provinces = pd.read_csv("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_provincial_cumulative_timeline_confirmed.csv")
df_provinces = df_provinces.drop(columns=['source','total','UNKNOWN'])


# In[11]:


from database import model as m
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


secret = {
    "username": "admin",
    "password": "DRgPXv9mLuF0fsplWnox",
    "host": "database-2.cnzbp4ndrpos.eu-west-1.rds.amazonaws.com",
    "port": "1433"
}

engine = create_engine(
    'mssql+pymssql://' +
    secret['username'] + ':' + secret['password'] + '@' + secret['host'] + ':' +
    str(secret['port']) + '/Corona'

)

session = sessionmaker()(bind=engine)


# In[12]:


CasesGlobal_df = pd.read_sql_query("SELECT * FROM CasesGlobal", engine)
CasesLocal_df = pd.read_sql_query("SELECT * FROM CasesLocal", engine)
Tests_df = pd.read_sql_query("SELECT * FROM Tests", engine)


CasesGlobal_df.to_csv('CasesGlobal.csv', index=False)
CasesLocal_df.to_csv('CasesLocal.csv', index=True)
Tests_df.to_csv('Tests.csv', index=False)