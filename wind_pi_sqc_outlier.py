export SPARK_MAJOR_VERSION=2
pyspark --num-executors 5 --driver-memory 6g --executor-memory 6g

from pyspark.sql.types import IntegerType,TimestampType,FloatType
import pyspark.sql.functions as F
from pyspark.sql.functions import substring
from pyspark.sql.functions import year, month, dayofmonth,unix_timestamp
from pyspark.sql.functions import col,when,max
from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.sql.functions import array, col, explode, struct, lit,avg
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import array,udf
import numpy as np
import sys

#spark = SparkSession.builder.appName("wind_pi_eda").enableHiveSupport().getOrCreate()
df = spark.sql("select * from analytical_ds.wind_turbine_combined_data")


split_col = F.split(df['tur_date'], '-')
df = df.withColumn('month', split_col.getItem(1))
df = df.withColumn('date', split_col.getItem(0))
split_col_2 = F.split(df['tur_time'], ':')
df = df.withColumn('hour', split_col_2.getItem(0))
df = df.withColumn('minute', split_col_2.getItem(1))
df = df.withColumn('datetime_index', F.concat(F.col('date'),F.lit('-'), F.col('month'),F.lit('-2017 '), F.col('hour'),F.lit(':'),F.col('minute'),F.lit(':00')))

col_names = df.drop('ext_curtailment_ind_avg','int_derate_ind_avg','consumption_counter_sample','row_num','operating_state','state_fault').columns[3:-5]


for i in range(0,len(col_names)):
	df2 = df.select(unix_timestamp('datetime_index', "dd-MMM-yyyy HH:mm:ss") .cast(TimestampType()).alias("timestamp"),'turbine_id',col_names[i])
	df2 = df2.withColumn(col_names[i], F.last(col_names[i], True).over(Window.partitionBy('turbine_id').orderBy('timestamp').rowsBetween(-sys.maxsize, 0)))
	df2 = df2.withColumn('lag1',F.lag(df2[col_names[i]]).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag2',F.lag(df2[col_names[i]],2).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag3',F.lag(df2[col_names[i]],3).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag4',F.lag(df2[col_names[i]],4).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag5',F.lag(df2[col_names[i]],5).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag6',F.lag(df2[col_names[i]],6).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag7',F.lag(df2[col_names[i]],7).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag8',F.lag(df2[col_names[i]],8).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag9',F.lag(df2[col_names[i]],9).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag10',F.lag(df2[col_names[i]],10).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag11',F.lag(df2[col_names[i]],11).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lag12',F.lag(df2[col_names[i]],12).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead1',F.lead(df2[col_names[i]]).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead2',F.lead(df2[col_names[i]],2).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead3',F.lead(df2[col_names[i]],3).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead4',F.lead(df2[col_names[i]],4).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead5',F.lead(df2[col_names[i]],5).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead6',F.lead(df2[col_names[i]],6).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead7',F.lead(df2[col_names[i]],7).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead8',F.lead(df2[col_names[i]],8).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead9',F.lead(df2[col_names[i]],9).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead10',F.lead(df2[col_names[i]],10).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead11',F.lead(df2[col_names[i]],11).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	df2 = df2.withColumn('lead12',F.lead(df2[col_names[i]],12).over(Window.partitionBy("turbine_id").orderBy("timestamp")))
	combined = array(*(col(x) for x in df2.columns[3:]))
	mean_udf = udf(lambda xs: float(np.mean(xs)), FloatType())
	std_udf = udf(lambda xs: float(np.std(xs,ddof=1)), FloatType())
	df3 = df2.where((col('lag12').isNotNull()) & (col('lead12').isNotNull())).select('timestamp','turbine_id',col_names[i],(mean_udf(combined)+3*(std_udf(combined))).alias('upper_limit'),(mean_udf(combined)-3*(std_udf(combined))).alias('lower_limit'))
	df_outlier=df3.select('timestamp','turbine_id',col_names[i]).where((col(col_names[i])>col('upper_limit')) | (col(col_names[i])<col('lower_limit')))
	if i==0:
		df_outlier_temp=df_outlier
	else:
		df_outlier_temp = df_outlier_temp.alias('a').join(df_outlier.alias('b'),[df_outlier.timestamp == df_outlier_temp.timestamp,df_outlier.turbine_id == df_outlier_temp.turbine_id],how='full').select([F.coalesce(col('a.timestamp'),col('b.timestamp')).alias('timestamp')] + [F.coalesce(col('a.turbine_id'),col('b.turbine_id')).alias('turbine_id')] + [col('a.'+ xx) for xx in df_outlier_temp.drop('timestamp','turbine_id').columns] + [col(col_names[i])])

df_final_export = 	df_export1.append([df_export2,df_export3,df_export4,df_export5,df_export6])
p_schema = StructType([StructField('util_id',StringType(),True),StructField('cluster_name',StringType(),True),StructField('window',StringType(),True),StructField('model_no',IntegerType(),True),StructField('xfmr_phase',StringType(),True)])
final = sqlContext.createDataFrame(df_final_export, p_schema)
df_outlier_temp.registerTempTable("df_outlier_temp1") 
spark.sql("create table analytical_ds.wind_turbine_outlier_sqc_temp_2_hr as select * from df_outlier_temp1")
