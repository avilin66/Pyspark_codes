# -*- coding: utf-8 -*-
"""
Created on Fri Nov  1 12:04:41 2019

@author: 725292
"""


import pandas as pd
from matplotlib import pyplot
from statsmodels.tsa.seasonal import seasonal_decompose
from time import strptime
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

path = ''

df_raw = pd.read_csv(path+'wind_turbine_raw_data.csv')

df_raw['month'] = df_raw['tur_date'].apply(lambda x: strptime(x.split('-')[1],'%b').tm_mon)
df_raw['day'] = df_raw['tur_date'].apply(lambda x: int(x.split('-')[0]))
df_raw['hour'] = df_raw['tur_time'].apply(lambda x: int(x.split(':')[0]))
df_raw['minute'] = df_raw['tur_time'].apply(lambda x: int(x.split(':')[1]))
df_raw['year'] = 2017
df_raw['second']=0


df_raw['timestamp_str'] =  df_raw['month'].apply(lambda x: str(x))+"/"+df_raw['day'].apply(lambda x: str(x))+"/"+df_raw['year'].apply(lambda x: str(x))+" "+df_raw['hour'].apply(lambda x: str(x))+":"+df_raw['minute'].apply(lambda x: str(x))+":"+df_raw['second'].apply(lambda x: str(x))
df_raw['timestamp']=df_raw['timestamp_str'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'))

df_raw = df_raw.sort_values(['month','day','hour','minute'],ascending=[True,True,True,True])
df_raw = df_raw.reset_index()

a = list(df_raw.iloc[:, 4:-8].columns.values)

for i in range(0,len(a)):
    df_series = df_raw[['timestamp',a[i]]]
    df_series = df_series.set_index(df_series.columns[0])
    df_series.index = pd.to_datetime(df_series.index)
    df_series.fillna(method='ffill', inplace=True)
    result = seasonal_decompose(df_series, model='additive',freq=4320)
    plot1 = result.plot()
    plot1.savefig(path+a[i]+'.jpg')
    
    x = np.array(result.resid[result.resid[a[i]].notnull()])
    mean = np.mean(x)
    std_dev=np.std(x)
    upper_limit = mean+3*(std_dev)
    lower_limit = mean-3*(std_dev)
    df_outlier_temp = result.resid[(result.resid[a[i]] > upper_limit) | (result.resid[a[i]] < lower_limit)]
    df_outlier_observed = df_outlier_temp.merge(result.observed, left_index=True, right_index=True).iloc[:,1]
    if i==0:
        df_outlier_final = df_outlier_observed
    else:
        df_outlier_final = pd.concat([df_outlier_final,df_outlier_observed],axis=1)
        
df_outlier_final.to_csv(path+'/wind_turbine_outlier_data.csv')
