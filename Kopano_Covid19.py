#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd

df_C = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv")
df_D = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv")
recovered_df = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv")
df_tests = pd.read_csv("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_testing.csv")
df_provinces = pd.read_csv("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_provincial_cumulative_timeline_confirmed.csv")
df_provinces = df_provinces.drop(columns=['source','total','UNKNOWN'])


# In[2]:


from database import model as m
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pyodbc

connection = pyodbc.connect(driver='{SQL Server}',
                      host='DATABASE-5',
                      database='Corona',
                      trusted_connection='tcon',
                      user='sa')

engine = create_engine('mssql+pyodbc://sa:iT@9767398@DATABASE-5/Corona?driver=SQL+Server')
session = sessionmaker()(bind=engine)


# In[18]:


CasesGlobal_df = pd.read_sql_query("SELECT * FROM CasesGlobal", connection)
CasesLocal_df = pd.read_sql_query("SELECT * FROM CasesLocal", connection)
Tests_df = pd.read_sql_query("SELECT * FROM Tests", connection)


# ### Clean Confirmed Cases

# In[4]:


df_C = df_C.melt(id_vars = ["Country/Region","Lat","Long","Province/State"])

df_C_country_level = df_C.groupby(["Country/Region","variable"]).sum()

df_C_country_level = df_C_country_level.reset_index()

df_C_country_level["Date"] = df_C_country_level.variable.apply(lambda x: pd.to_datetime(x))

df_C_country_level = df_C_country_level[df_C_country_level["value"]!=0]


# ### Clean Deaths

# In[5]:


df_D = df_D.melt(id_vars = ["Country/Region","Lat","Long","Province/State"])

df_D_country_level = df_D.groupby(["Country/Region","variable"]).sum()

df_D_country_level = df_D_country_level.reset_index()

df_D_country_level["Date"] = df_D_country_level.variable.apply(lambda x: pd.to_datetime(x))

df_D_country_level = df_D_country_level[df_D_country_level["value"]!=0]


# ### Clean Recovered

# In[6]:


recovered_df = recovered_df.melt(id_vars = ["Country/Region","Lat","Long","Province/State"])

recovered_df_country_level = recovered_df.groupby(["Country/Region","variable"]).sum()

recovered_df_country_level = recovered_df_country_level.reset_index()

recovered_df_country_level["Date"] = recovered_df_country_level.variable.apply(lambda x: pd.to_datetime(x))

recovered_df_country_level = recovered_df_country_level[recovered_df_country_level["value"]!=0]


# ### Merge Deaths with confirmed cases and with recovered

# In[7]:


df_merge = df_C_country_level.merge(df_D_country_level ,on = ["Country/Region","Date"],how = "left")
df_merge2 = df_D_country_level.merge(recovered_df_country_level, on = ["Country/Region", "Date"], how = "left")


# ### Clean merged data

# In[8]:


def error(x):
    if pd.isnull(x):
        return 0
    else:
        return x
    
df_merge["deaths"] = df_merge.value_y.apply(lambda x: error(x))
df_merge["recovered"] = df_merge2.value_x.apply(lambda x: error(x))


# ### Upload Global Data

# In[9]:


for i in range(len(df_merge)):
    
    if df_merge["Date"][i] > pd.to_datetime(CasesGlobal_df["date"]).max():
    
    ## Check Country exisits
        Country = session.query(m.Country).filter(m.Country.country == df_merge["Country/Region"][i]).first()
        if Country is None:
            Country = m.Country(country = df_merge["Country/Region"][i])
            session.add(Country)
            session.commit()

        CasesGlobal = m.CasesGlobal( 
                            date = df_merge["Date"][i],
                            confirmed = int(df_merge["value_x"].fillna(0)[i]),
                            deaths = int(df_merge["deaths"].fillna(0)[i]),
                            recovered = int(df_merge["recovered"].fillna(0)[i]),
                            country_id = Country.id
        )
        session.add(CasesGlobal)
        session.commit()
    else:
        pass
    
session.commit()
session.close()


# ### Clean SA Tests Data

# In[10]:


df_tests["date"] = pd.to_datetime(df_tests["date"], dayfirst=True)
def error(x):
    if pd.isnull(x):
        return 0
    else:
        return x
    
df_tests["cumulative_tests"] = df_tests.cumulative_tests.apply(lambda x: error(x))
df_tests = df_tests.drop(columns=['source'])


# ### Clean Provincial Data

# In[11]:



df_provinces["date"] = pd.to_datetime(df_provinces["date"], dayfirst=True)
df_provinces = df_provinces.melt(id_vars = ["date","YYYYMMDD"])


# ### Upload Provincial Data

# In[12]:


Upload_Country = "South Africa"
level = "Provincial"
Upload_Country_id = session.query(m.Country).filter(m.Country.country == Upload_Country).first().id

for i in range(len(df_provinces)):
    
    if df_provinces["date"][i] > pd.to_datetime(CasesLocal_df["date"]).max():
    
    ## Check Country exisits
        Location = session.query(m.Location).filter(m.Location.location == df_provinces["variable"][i]).first()
        if Location is None:
            Location = m.Location(country_id = Upload_Country_id,
                                  location = df_provinces["variable"][i],
                                  location_level = level)
            session.add(Location)
            session.commit()
        CasesLocal = m.CasesLocal( 
                        date = df_provinces["date"][i],
                        confirmed = int(df_provinces["value"].fillna(0)[i]),
                        location_id = Location.id
        )
        session.add(CasesLocal)
    else:
        pass
session.commit()
session.close()


# In[13]:


Upload_Country = "South Africa"

for i in range(len(df_tests)):
    
    if df_tests["date"][i] > pd.to_datetime(Tests_df["date"]).max():
    ## Check Country exisits
        Country = session.query(m.Country).filter(m.Country.country == Upload_Country).first()
        if Country is None:
            Print("Country Not Found")
        Tests = m.Tests( 
                            date = df_tests["date"][i],
                            cumulative_tests = int(df_tests["cumulative_tests"].fillna(0)[i]),
                            country_id = Country.id
        )

        session.add(Tests)
    else:
        pass
session.commit()
session.close()


# In[14]:


df_tests["date"].max()


# In[19]:


Accum_cases = CasesLocal_df.groupby("date").sum()
Accum_cases["daily difference"] = Accum_cases["confirmed"].diff()


# In[20]:


Accum_cases = Accum_cases.drop(columns=["id","location_id"])
Accum_cases


# In[17]:


Accum_cases = Accum_cases.reset_index()
Accum_cases


# In[ ]:




