
from sklearn.neural_network import MLPRegressor
from pyspark import sql
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F 
from pyspark.sql.types import StringType
from pyspark.sql.types import DateType
from pyspark.sql.functions import date_format
from pyspark.sql.types import ArrayType, StringType, IntegerType, DoubleType, LongType, FloatType
from pyspark.sql.types import *
from pyspark.sql.functions import date_format
from datetime import datetime
from pyspark.sql.functions import sum,trim,udf,lit
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import isnan, when, count, col
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
import numpy as np
import pandas as pd
from operator import add
from functools import reduce
from pyspark.ml.feature import Bucketizer
from pyspark.ml.feature import StringIndexer
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler 
import keras
from keras.models import Model
from keras.layers import Dense,Activation,Input
from keras.callbacks import ModelCheckpoint
from sklearn.model_selection import train_test_split
from keras.models import load_model
from sklearn.externals import joblib


if __name__ == "__main__":
    conf = SparkConf().setAppName("ann_model_prediction")
    
sc= SparkContext(conf=conf)
sqlContext = HiveContext(sc)
    
spark = SparkSession.builder.appName("ann_model_prediction").enableHiveSupport().getOrCreate()


#df19['ml_score'] = (df19['prediction']/30)*12 + df19['usage_till_date']

#df19['ma_score'] = df19['first7day_avg']*12 + df19['usage_till_date']



df = spark.sql("select * from analytical_ds.euw_model_input_data where day_num >= 8 and day_num <= 21")
df = df.toPandas()


df['usage_till_date'] = df['usage_till_date'].convert_objects(convert_numeric=True)
df['first7day_avg'] = df['first7day_avg'].convert_objects(convert_numeric=True)
df['last_1bc_usg'] = df['last_1bc_usg'].convert_objects(convert_numeric=True)

#### day 8 predictions
df8 = df[df['day_num'] ==8]

scalerX8 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day8') 
scalery8 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day8') 
model_d8 = load_model('/data01/data/dev/euw_ann/model_objects/day_8_model')


features_df8 = df8[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X8 = features_df8.as_matrix(columns=features_df8.columns[0:])
 
X8_test = scalerX8.transform(X8)
pred8 = model_d8.predict(X8_test)
prediction8 = scalery8.inverse_transform(pred8)

df8['prediction'] = prediction8

#### day 9 predictions
df9 = df[df['day_num'] ==9]

scalerX9 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day9') 
scalery9 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day9') 
model_d9 = load_model('/data01/data/dev/euw_ann/model_objects/day_9_model')


features_df9 = df9[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X9 = features_df9.as_matrix(columns=features_df9.columns[0:])
 
X9_test = scalerX9.transform(X9)
pred9 = model_d9.predict(X9_test)
prediction9 = scalery9.inverse_transform(pred9)

df9['prediction'] = prediction9

### day 10 predictions 

df10 = df[df['day_num'] ==10]

scalerX10 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day10') 
scalery10 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day10') 
model_d10 = load_model('/data01/data/dev/euw_ann/model_objects/day_10_model')


features_df10 = df10[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X10 = features_df10.as_matrix(columns=features_df10.columns[0:])
 
X10_test = scalerX10.transform(X10)
pred10 = model_d10.predict(X10_test)
prediction10 = scalery10.inverse_transform(pred10)

df10['prediction'] = prediction10

### day 11 predictions

df11 = df[df['day_num'] ==11]

scalerX11 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day11') 
scalery11 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day11') 
model_d11 = load_model('/data01/data/dev/euw_ann/model_objects/day_11_model')


features_df11 = df11[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X11 = features_df11.as_matrix(columns=features_df11.columns[0:])
 
X11_test = scalerX11.transform(X11)
pred11 = model_d11.predict(X11_test)
prediction11 = scalery11.inverse_transform(pred11)

df11['prediction'] = prediction11


### day 12 predictions

df12 = df[df['day_num'] ==12]

scalerX12 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day12') 
scalery12 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day12') 
model_d12 = load_model('/data01/data/dev/euw_ann/model_objects/day_12_model')


features_df12 = df12[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X12 = features_df12.as_matrix(columns=features_df12.columns[0:])
 
X12_test = scalerX12.transform(X12)
pred12 = model_d12.predict(X12_test)
prediction12 = scalery12.inverse_transform(pred12)

df12['prediction'] = prediction12

## day 13 predictions 

df13 = df[df['day_num'] ==13]

scalerX13 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day13') 
scalery13 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day13') 
model_d13 = load_model('/data01/data/dev/euw_ann/model_objects/day_13_model')


features_df13 = df13[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X13 = features_df13.as_matrix(columns=features_df13.columns[0:])
 
X13_test = scalerX13.transform(X13)
pred13 = model_d13.predict(X13_test)
prediction13 = scalery13.inverse_transform(pred13)

df13['prediction'] = prediction13

## day 14 predictions

df14 = df[df['day_num'] ==14]

scalerX14 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day14') 
scalery14 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day14') 
model_d14 = load_model('/data01/data/dev/euw_ann/model_objects/day_14_model')


features_df14 = df14[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X14 = features_df14.as_matrix(columns=features_df14.columns[0:])
 
X14_test = scalerX14.transform(X14)
pred14 = model_d14.predict(X14_test)
prediction14 = scalery14.inverse_transform(pred14)

df14['prediction'] = prediction14

## day 15 prediction

df15 = df[df['day_num'] ==15]

scalerX15 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day15') 
scalery15 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day15') 
model_d15 = load_model('/data01/data/dev/euw_ann/model_objects/day_15_model')


features_df15 = df15[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X15 = features_df15.as_matrix(columns=features_df15.columns[0:])
 
X15_test = scalerX15.transform(X15)
pred15 = model_d15.predict(X15_test)
prediction15 = scalery15.inverse_transform(pred15)

df15['prediction'] = prediction15

## day 16 prediction

df16 = df[df['day_num'] ==16]

scalerX16 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day16') 
scalery16 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day16') 
model_d16 = load_model('/data01/data/dev/euw_ann/model_objects/day_16_model')


features_df16 = df16[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X16 = features_df16.as_matrix(columns=features_df16.columns[0:])
 
X16_test = scalerX16.transform(X16)
pred16 = model_d16.predict(X16_test)
prediction16 = scalery16.inverse_transform(pred16)

df16['prediction'] = prediction16

## day 17 prediction 

df17 = df[df['day_num'] ==17]

scalerX17 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day17') 
scalery17 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day17') 
model_d17 = load_model('/data01/data/dev/euw_ann/model_objects/day_17_model')


features_df17 = df17[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X17 = features_df17.as_matrix(columns=features_df17.columns[0:])
 
X17_test = scalerX17.transform(X17)
pred17 = model_d17.predict(X17_test)
prediction17 = scalery17.inverse_transform(pred17)

df17['prediction'] = prediction17

## day 18 prediction 

df18 = df[df['day_num'] ==18]

scalerX18 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day18') 
scalery18 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day18') 
model_d18 = load_model('/data01/data/dev/euw_ann/model_objects/day_18_model')


features_df18 = df18[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X18 = features_df18.as_matrix(columns=features_df18.columns[0:])
 
X18_test = scalerX18.transform(X18)
pred18 = model_d18.predict(X18_test)
prediction18 = scalery18.inverse_transform(pred18)

df18['prediction'] = prediction18

## day 19 prediction

df19 = df[df['day_num'] ==19]

scalerX19 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day19') 
scalery19 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day19') 
model_d19 = load_model('/data01/data/dev/euw_ann/model_objects/day_19_model')


features_df19 = df19[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X19 = features_df19.as_matrix(columns=features_df19.columns[0:])
 
X19_test = scalerX19.transform(X19)
pred19 = model_d19.predict(X19_test)
prediction19 = scalery19.inverse_transform(pred19)

df19['prediction'] = prediction19

## day 20 prediction

df20 = df[df['day_num'] ==20]

scalerX20 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day20') 
scalery20 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day20') 
model_d20 = load_model('/data01/data/dev/euw_ann/model_objects/day_20_model')


features_df20 = df20[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X20 = features_df20.as_matrix(columns=features_df20.columns[0:])
 
X20_test = scalerX20.transform(X20)
pred20 = model_d20.predict(X20_test)
prediction20 = scalery20.inverse_transform(pred20)

df20['prediction'] = prediction20

## day 21 prediction

df21 = df[df['day_num'] ==21]

scalerX21 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalerx_day21') 
scalery21 = joblib.load('/data01/data/dev/euw_ann/model_objects/scalery_day21') 
model_d21 = load_model('/data01/data/dev/euw_ann/model_objects/day_21_model')


features_df21 = df21[[
'usage_till_date',
'last_1bc_usg',
'average_temp_prev',
'min_temp_prev',
'max_temp_prev',
'average_dpt_prev',
'average_hum_prev',
'average_temp_fut',
'min_temp_fut',
'max_temp_fut',
'holidays_prev',
'holidays_fut',
'weekends_prev',
'weekends_fut',
'average_dpt_fut',
'average_hum_fut']]

X21 = features_df21.as_matrix(columns=features_df21.columns[0:])
 
X21_test = scalerX21.transform(X21)
pred21 = model_d21.predict(X21_test)
prediction21 = scalery21.inverse_transform(pred21)

df21['prediction'] = prediction21


df_new = pd.concat([df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,df19,df20,df21])

df_new['ml_score'] = (df_new['prediction']/30)*(31 - df_new['day_num']) + df_new['usage_till_date']

df_new['ma_score'] = df_new['first7day_avg']*(31 - df_new['day_num']) + df_new['usage_till_date']


p_schema = StructType([StructField('concat_agmnt_no',StringType(),True),StructField('billing_start_date',StringType(),True),StructField('billing_end_date',StringType(),True),StructField('day_num',IntegerType(),True),StructField('usage_till_date',FloatType(),True),StructField('first7day_avg',FloatType(),True),StructField('last_1bc_usg',FloatType(),True),StructField('ml_score',FloatType(),True),StructField('ma_score',FloatType(),True)])
 

df1 = df_new[['concat_agmnt_no',
'billing_start_date',
'billing_end_date',
'day_num',
'usage_till_date',
'first7day_avg',
'last_1bc_usg',
'ml_score',
'ma_score']]
 
df2 = sqlContext.createDataFrame(df1, p_schema)
 
df2.registerTempTable("test_temp_df") 

spark.sql("create table analytical_ds.euw_model_output_data as select a.*, to_Date(from_unixtime(unix_timestamp())) as run_date from test_temp_df a ")




