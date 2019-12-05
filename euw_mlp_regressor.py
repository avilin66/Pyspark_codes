export SPARK_MAJOR_VERSION=2
pyspark --num-executors 5 --driver-memory 6g --executor-memory 6g
 
 
from sklearn.neural_network import MLPRegressor
from pyspark import sql
from pyspark import SparkKontext, SparkKonf
from pyspark.sql import HiveKontext
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
 
 
train = spark.sql("select * from analytical_ds.euw_enhancement where billing_cycle != 'BC_1' and actual_usage >= 200 and actual_usage <= 2000 ")
test = spark.sql("select * from analytical_ds.euw_enhancement where billing_cycle = 'BC_1' and last_1bc_usg >= 200 and last_1bc_usg <= 2000 ")
 
 
train = train.toPandas()
test = test.toPandas()
 
 
features_df = train[['first7day_avg',
'usage_till_date',
'average_temp',
'min_temp',
'max_temp',
'average_dpt',
'average_hum',
'holidays',
'last_1bc_first7day_avg',
'last_1bc_usg',
'last_2bc_first7day_avg',
'last_2bc_usg',
'last_1bc_average_temp',
'last_1bc_min_temp',
'last_1bc_max_temp',
'last_1bc_average_dpt',
'last_1bc_average_hum',
'last_2bc_average_temp',
'last_2bc_min_temp',
'last_2bc_max_temp',
'last_2bc_average_dpt',
'last_2bc_average_hum',
'last_1bc_holidays',
'last_2bc_holidays']]
 
 
X = features_df.as_matrix(columns=features_df.columns[0:])
Y = train['actual_usage'].as_matrix()
 
Y = np.array(Y).reshape(-1,1)


scalerX = StandardScaler().fit(X)
scalery = StandardScaler().fit(Y)
X_train = scalerX.transform(X)
y_train = scalery.transform(Y)


y_train_v1 = y_train.ravel()

mlp = MLPRegressor(hidden_layer_sizes=(24,24,24),max_iter=1000)
 
mlp = mlp.fit(X_train,y_train_v1)
 
 
features_df_test = test[['first7day_avg',
'usage_till_date',
'average_temp',
'min_temp',
'max_temp',
'average_dpt',
'average_hum',
'holidays',
'last_1bc_first7day_avg',
'last_1bc_usg',
'last_2bc_first7day_avg',
'last_2bc_usg',
'last_1bc_average_temp',
'last_1bc_min_temp',
'last_1bc_max_temp',
'last_1bc_average_dpt',
'last_1bc_average_hum',
'last_2bc_average_temp',
'last_2bc_min_temp',
'last_2bc_max_temp',
'last_2bc_average_dpt',
'last_2bc_average_hum',
'last_1bc_holidays',
'last_2bc_holidays']]
 
X1 = features_df_test.as_matrix(columns=features_df_test.columns[0:])
 
X_test = scalerX.transform(X1)
 
test_y = mlp.predict(X_test)

Y = np.array(Y).reshape(-1,1)

test_y_new = scalery.inverse_transform(test_y)


#X_test = scaler.transform(X1)
#test_y = mlp.predict(X_test)
#test["prediction"] = test_y
 
 
test["prediction"] = test_y_new
 
p_schema = StructType([StructField('concat_agmnt_no',StringType(),True),StructField('billing_start_date',StringType(),True),StructField('billing_end_date',StringType(),True),StructField('usage_till_date',DecimalType(),True),StructField('first7day_avg',DecimalType(),True),StructField('actual_usage',DecimalType(),True),StructField('prediction',FloatType(),True)])
 
 
test_v1 = test[['concat_agmnt_no',
'billing_start_date',
'billing_end_date',
'usage_till_date',
'first7day_avg',
'actual_usage',
'prediction']]
 
 
test_v2 = sqlContext.createDataFrame(test_v1, p_schema)
 
temp_df = test_v2.select('concat_agmnt_no','billing_start_date','billing_end_date','usage_till_date','first7day_avg','actual_usage','prediction')
temp_df.registerTempTable("test_temp_df")
 
df = spark.sql("select a.*, a.usage_till_date + (23*a.first7day_avg) as ma_score from test_temp_df a ")
df.registerTempTable("temp_df")
 

df1 = spark.sql("select a.concat_agmnt_no, a.usage_till_date,a.first7day_avg, a.actual_usage, a.prediction, a.ma_score, 1 - (abs(a.prediction - a.actual_usage)/a.actual_usage) as ml_accuracy, 1 - (abs(a.ma_score - a.actual_usage)/a.actual_usage) as ma_accuracy from temp_df a")

df1.registerTempTable("temp_df1")


spark.sql("select count(distinct concat_agmnt_no) as total_agreements, count(distinct(case when ml_accuracy >= ma_accuracy then concat_agmnt_no end)) as ML_better_agreements from temp_df1").show()




