from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from pyspark.sql.functions import substring
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import col,when,max,countDistinct
from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.sql.functions import array, col, explode, struct, lit,avg
from pyspark.sql.window import Window
from sklearn.cluster import SpectralClustering
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import numpy as np
from pyspark.sql.functions import monotonically_increasing_id
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession


if __name__ == "__main__":
    conf = SparkConf().setAppName("spectral_clustering")
    
sc= SparkContext(conf=conf)
sqlContext = HiveContext(sc)
    
spark = SparkSession.builder.appName("spectral_clustering").enableHiveSupport().getOrCreate()


df = spark.sql("select * from analytical_ds.cust_phase_model_input_data where length(trim(xfmr_phase))=1 order by util_id,read_start_date,read_start_time")  
df = df.withColumn("hour", substring(F.col("read_start_time"), 0, 2))
df = df.withColumn("mth",month("read_start_date"))
df = df.withColumn("day",F.format_string("%02d",dayofmonth("read_start_date")))
df = df.withColumn('datetime_index', F.concat(F.col('mth'),F.lit('_'), F.col('day'),F.lit('_'), F.col('hour')))

n1 = df.select("read_start_date").distinct().count()

df_x1 = df.select("feeder_id","util_id","xfmr_phase").distinct()
#df_pd = df_x1.toPandas()
    
df = df.withColumn('lag_volt',F.lag(df['voltage']).over(Window.partitionBy("util_id").orderBy("datetime_index")))
df = df.withColumn('voltage_diff',df['voltage']-df['lag_volt'])
df = df.where("voltage_diff is not null")
    
df_dist_1 = df.select("util_id","dist").distinct()
df_dist_2 = df.select("util_id","dist","xfmr_sub_dist").distinct()
    
unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())

for i in ["dist"]:
	# VectorAssembler Transformation - Converting column to vector type
	assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
	# MinMaxScaler Transformation
	scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
	# Pipeline of VectorAssembler and MinMaxScaler
	pipeline = Pipeline(stages=[assembler, scaler])
	# Fitting pipeline on dataframe
	df_dist_1_temp = pipeline.fit(df_dist_1).transform(df_dist_1).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")
        
        
    
for i in ["xfmr_sub_dist"]:
	# VectorAssembler Transformation - Converting column to vector type
	assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
	# MinMaxScaler Transformation
	scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
	# Pipeline of VectorAssembler and MinMaxScaler
	pipeline = Pipeline(stages=[assembler, scaler])
	# Fitting pipeline on dataframe
	df_temp_2 = pipeline.fit(df_dist_2).transform(df_dist_2).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")
        
df_dist_2_temp = df_dist_1_temp.join(df_temp_2, df_dist_1_temp.util_id == df_temp_2.util_id).select(df_dist_1_temp["*"],df_temp_2["xfmr_sub_dist_Scaled"]) 




majors = sorted(df.select("datetime_index").distinct().rdd.map(lambda row: row[0]).collect())

cols = [when(col("datetime_index") == m, col("voltage_diff")).otherwise(None).alias(m) for m in  majors]
maxs = [max(col(m)).alias(m) for m in majors]

grouped = (df.rdd.map(lambda row: (row.util_id, (row.datetime_index, row.voltage_diff))).groupByKey())

def make_row(kv):
	k, vs = kv
	tmp = dict(list(vs) + [("util_id", k)])
	return Row(**{k: tmp.get(k, 0) for k in ["util_id"] + majors})

df_del_volt_reshaped = sqlContext.createDataFrame(grouped.map(make_row))


cols = [when(col("datetime_index") == m, col("voltage")).otherwise(None).alias(m) for m in  majors]
maxs = [max(col(m)).alias(m) for m in majors]

grouped = (df.rdd.map(lambda row: (row.util_id, (row.datetime_index, row.voltage))).groupByKey())

df1_abs_volt_reshaped = sqlContext.createDataFrame(grouped.map(make_row))




assembler = VectorAssembler().setInputCols(df_del_volt_reshaped.columns[:-1]).setOutputCol("features")
transformed = assembler.transform(df_del_volt_reshaped)
scaler = MinMaxScaler(inputCol="features",outputCol="scaledFeatures")
scalerModel = scaler.fit(transformed.select("features"))
scaledData = scalerModel.transform(transformed)


def extract(row):
	return tuple(row.scaledFeatures.toArray().tolist())+(row.util_id, )


               
               
               
               
def to_long(df, by):
	# Filter dtypes and split into column names and type description
	cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
	# Spark SQL supports only homogeneous columns
	assert len(set(dtypes)) == 1, "All columns have to be of the same type"	
	# Create and explode an array of (column_name, column_value) structs
	kvs = explode(array([struct(lit(c).alias("key"), col(c).alias("val")) for c in cols])).alias("kvs")
	return df.select(by + [kvs]).select(by + ["kvs.key", "kvs.val"])
   
df2_del_volt = scaledData.select("scaledFeatures","util_id").rdd.map(extract).toDF(df_del_volt_reshaped.columns)
df2_long_del_volt = to_long(df2_del_volt, ["util_id"])


assembler = VectorAssembler().setInputCols(df1_abs_volt_reshaped.columns[:-1]).setOutputCol("features")
transformed = assembler.transform(df1_abs_volt_reshaped)
scaler = MinMaxScaler(inputCol="features",outputCol="scaledFeatures")
scalerModel = scaler.fit(transformed.select("features"))
scaledData = scalerModel.transform(transformed) 
    
df2_abs_volt = scaledData.select("scaledFeatures","util_id").rdd.map(extract).toDF(df1_abs_volt_reshaped.columns)
df2_long_abs_volt = to_long(df2_abs_volt, ["util_id"])
    

	
df2_long_del_volt.createOrReplaceTempView('df_temp_del_volt')
df3_long_del_volt = spark.sql("select a.*, row_number() over (partition by a.util_id order by a.key) as row_num from (select * from df_temp_del_volt)a")
df3_long_del_volt = df3_long_del_volt.withColumn("window",(F.col('row_num')/24).cast(IntegerType()))
df2_long_v1_del = df3_long_del_volt.where("val is not null")
df2_long_v1_del.createOrReplaceTempView('df_temp_test_del') 
df2_long_v1_del = spark.sql("select a.*, row_number() over (partition by a.util_id,a.window order by a.key) as key_value from (select * from df_temp_test_del)a")
df_test_1_del = df2_long_v1_del.groupby('util_id','window').pivot('key_value').max('val').fillna(0) 

df1_r_del = df_test_1_del.select(*(col(x).alias(x+'_df1') for x in df_test_1_del.columns))
df2_r_del = df_test_1_del.select(*(col(x).alias(x+'_df2') for x in df_test_1_del.columns))
df3_r_del = df_test_1_del.select(*(col(x).alias(x+'_df3') for x in df_test_1_del.columns))

df1_r_del.createOrReplaceTempView('df1_temp_del') 
df2_r_del.createOrReplaceTempView('df2_temp_del') 
df3_r_del.createOrReplaceTempView('df3_temp_del') 

df_long_v2_del = spark.sql("select x1.*, x2.* from (select a.*, b.* from (select * from df1_temp_del)a left join (select * from df2_temp_del)b on a.util_id_df1 = b.util_id_df2 and a.window_df1 +1 = b.window_df2)x1 left join df3_temp_del x2 on x1.util_id_df1 = x2.util_id_df3 and x1.window_df1 +2 = x2.window_df3")

df_long_v2_del = df_long_v2_del.orderBy('window_df1','util_id_df1')

df_Spectral_v1_del = df_long_v2_del.alias('a').join(df_dist_1_temp.alias('b'),col('a.util_id_df1')==col('b.util_id')).select([col('a.'+ xx) for xx in df_long_v2_del.columns] + [col('b.dist_scaled')])
df_Spectral_v2_del = df_long_v2_del.alias('a').join(df_dist_2_temp.alias('b'),col('a.util_id_df1')==col('b.util_id')).select([col('a.'+ xx) for xx in df_long_v2_del.columns] + [col('b.dist_scaled'),col('b.xfmr_sub_dist_Scaled')])

df_long_v2_del = df_long_v2_del.orderBy('window_df1','util_id_df1')
df_Spectral_v1_del = df_Spectral_v1_del.orderBy('window_df1','util_id_df1')
df_Spectral_v2_del = df_Spectral_v2_del.orderBy('window_df1','util_id_df1')


df2_long_abs_volt.createOrReplaceTempView('df_temp_abs_volt')
df3_long_abs_volt = spark.sql("select a.*, row_number() over (partition by a.util_id order by a.key) as row_num from (select * from df_temp_abs_volt)a")
df3_long_abs_volt = df3_long_abs_volt.withColumn("window",(F.col('row_num')/24).cast(IntegerType()))

df2_long_v1_abs = df3_long_abs_volt.where("val is not null")
df2_long_v1_abs.createOrReplaceTempView('df_temp_test_abs') 
df2_long_v1_abs = spark.sql("select a.*, row_number() over (partition by a.util_id,a.window order by a.key) as key_value from (select * from df_temp_test_abs)a")
df_test_1_abs = df2_long_v1_abs.groupby('util_id','window').pivot('key_value').max('val').fillna(0) 


df1_r_abs = df_test_1_abs.select(*(col(x).alias(x+'_df1') for x in df_test_1_abs.columns))
df2_r_abs = df_test_1_abs.select(*(col(x).alias(x+'_df2') for x in df_test_1_abs.columns))
df3_r_abs = df_test_1_abs.select(*(col(x).alias(x+'_df3') for x in df_test_1_abs.columns))

df1_r_abs.createOrReplaceTempView('df1_temp_abs') 
df2_r_abs.createOrReplaceTempView('df2_temp_abs') 
df3_r_abs.createOrReplaceTempView('df3_temp_abs') 

df_long_v2_abs = spark.sql("select x1.*, x2.* from (select a.*, b.* from (select * from df1_temp_abs)a left join (select * from df2_temp_abs)b on a.util_id_df1 = b.util_id_df2 and a.window_df1 +1 = b.window_df2)x1 left join df3_temp_abs x2 on x1.util_id_df1 = x2.util_id_df3 and x1.window_df1 +2 = x2.window_df3")

df_long_v2_abs = df_long_v2_abs.orderBy('window_df1','util_id_df1')

df_Spectral_v1_abs = df_long_v2_abs.alias('a').join(df_dist_1_temp.alias('b'),col('a.util_id_df1')==col('b.util_id')).select([col('a.'+ xx) for xx in df_long_v2_abs.columns] + [col('b.dist_scaled')])
df_Spectral_v2_abs = df_long_v2_abs.alias('a').join(df_dist_2_temp.alias('b'),col('a.util_id_df1')==col('b.util_id')).select([col('a.'+ xx) for xx in df_long_v2_abs.columns] + [col('b.dist_scaled'),col('b.xfmr_sub_dist_Scaled')])

df_long_v2_abs = df_long_v2_abs.orderBy('window_df1','util_id_df1')
df_Spectral_v1_abs = df_Spectral_v1_abs.orderBy('window_df1','util_id_df1')
df_Spectral_v2_abs = df_Spectral_v2_abs.orderBy('window_df1','util_id_df1')


df_util = df.select("util_id").distinct().orderBy("util_id") 
df_util_pd = df_util.toPandas()
df_pd = df_x1.toPandas()
n = len(df_util_pd)

#x_test = np.array(df_long_v2_del.select(df_long_v2_del.drop('util_id_df2','window_df2','util_id_df3','window_df3').columns).collect())

x1 = np.array(df_long_v2_del.select(df_long_v2_del.drop('util_id_df1','window_df1','util_id_df2','window_df2','util_id_df3','window_df3').columns).collect())
x2 = np.array(df_Spectral_v1_del.select(df_Spectral_v1_del.drop('util_id_df1','window_df1','util_id_df2','window_df2','util_id_df3','window_df3').columns).collect())
x3 = np.array(df_Spectral_v2_del.select(df_Spectral_v2_del.drop('util_id_df1','window_df1','util_id_df2','window_df2','util_id_df3','window_df3').columns).collect())
x4 = np.array(df_long_v2_abs.select(df_long_v2_abs.drop('util_id_df1','window_df1','util_id_df2','window_df2','util_id_df3','window_df3').columns).collect())
x5 = np.array(df_Spectral_v1_abs.select(df_Spectral_v1_abs.drop('util_id_df1','window_df1','util_id_df2','window_df2','util_id_df3','window_df3').columns).collect())
x6 = np.array(df_Spectral_v2_abs.select(df_Spectral_v2_abs.drop('util_id_df1','window_df1','util_id_df2','window_df2','util_id_df3','window_df3').columns).collect())


for i in range(0,n1-2):
    print(i)
    if (i == 0):
        x1_1 = x1[0:n]
        x1_2 = x2[0:n]
        x1_3 = x3[0:n]
        x1_4 = x4[0:n]
        x1_5 = x5[0:n]
        x1_6 = x6[0:n]
    else:
        lower = (i*n)
        upper = (i+1)*n
        x1_1 = x1[lower:upper]
        x1_2 = x2[lower:upper]
        x1_3 = x3[lower:upper]
        x1_4 = x4[lower:upper]
        x1_5 = x5[lower:upper]
        x1_6 = x6[lower:upper]
    clustering = SpectralClustering(n_clusters=25,affinity="rbf",assign_labels="kmeans",random_state=0).fit(x1_1)
    df_final_model_1 = df_util_pd[['util_id']]
    label = clustering.labels_
    df_final_model_1['cluster'] = label
    df_final_model_1['window'] = i
    df_final_model_1['model_no'] = 1
    df1_export = df_final_model_1.merge(df_pd, on= 'util_id',how = 'left')
    if(i==0):
        df_export1 =df1_export
    else:
        df_export1 = df_export1.append(df1_export)
    clustering = SpectralClustering(n_clusters=25,affinity="rbf",assign_labels="kmeans",random_state=0).fit(x1_2)
    df_final_model_2 = df_util_pd[['util_id']]
    label = clustering.labels_
    df_final_model_2['cluster'] = label
    df_final_model_2['window'] = i
    df_final_model_2['model_no'] = 2
    df2_export = df_final_model_2.merge(df_pd, on= 'util_id',how = 'left')
    if(i==0):
        df_export2 =df2_export
    else:
        df_export2 = df_export2.append(df2_export)
    clustering = SpectralClustering(n_clusters=25,affinity="rbf",assign_labels="kmeans",random_state=0).fit(x1_3)
    df_final_model_3 = df_util_pd[['util_id']]
    label = clustering.labels_
    df_final_model_3['cluster'] = label
    df_final_model_3['window'] = i
    df_final_model_3['model_no'] = 3
    df3_export = df_final_model_3.merge(df_pd, on= 'util_id',how = 'left')
    if(i==0):
        df_export3 =df3_export
    else:
        df_export3 = df_export3.append(df3_export)
    clustering = SpectralClustering(n_clusters=25,affinity="rbf",assign_labels="kmeans",random_state=0).fit(x1_4)
    df_final_model_4 = df_util_pd[['util_id']]
    label = clustering.labels_
    df_final_model_4['cluster'] = label
    df_final_model_4['window'] = i
    df_final_model_4['model_no'] = 4
    df4_export = df_final_model_4.merge(df_pd, on= 'util_id',how = 'left')
    if(i==0):
        df_export4 =df4_export
    else:
        df_export4 = df_export4.append(df4_export)
    clustering = SpectralClustering(n_clusters=25,affinity="rbf",assign_labels="kmeans",random_state=0).fit(x1_5)
    df_final_model_5 = df_util_pd[['util_id']]
    label = clustering.labels_
    df_final_model_5['cluster'] = label
    df_final_model_5['window'] = i
    df_final_model_5['model_no'] = 5
    df5_export = df_final_model_5.merge(df_pd, on= 'util_id',how = 'left')
    if(i==0):
        df_export5 =df5_export
    else:
        df_export5 = df_export5.append(df5_export)
    clustering = SpectralClustering(n_clusters=25,affinity="rbf",assign_labels="kmeans",random_state=0).fit(x1_6)
    df_final_model_6 = df_util_pd[['util_id']]
    label = clustering.labels_
    df_final_model_6['cluster'] = label
    df_final_model_6['window'] = i
    df_final_model_6['model_no'] = 6
    df6_export = df_final_model_6.merge(df_pd, on= 'util_id',how = 'left')
    if(i==0):
        df_export6 =df6_export
    else:
        df_export6 = df_export6.append(df6_export)


	

df_final_export = 	df_export1.append([df_export2,df_export3,df_export4,df_export5,df_export6])
p_schema = StructType([StructField('util_id',StringType(),True),StructField('cluster_name',StringType(),True),StructField('window',StringType(),True),StructField('model_no',IntegerType(),True),StructField('feeder_id',IntegerType(),True),StructField('xfmr_phase',StringType(),True)])
final = sqlContext.createDataFrame(df_final_export, p_schema)
final.registerTempTable("final_temp_df")
spark.sql("drop table if exists analytical_ds.cust_phase_clustering_outptut_data") 
spark.sql("create table analytical_ds.cust_phase_clustering_outptut_data as select * from final_temp_df")
	
