from __future__ import print_function
from pyspark.sql import functions as F
from pyspark.sql.functions import mean, min, max, variance, lag, count, col
from pyspark import sql
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import ArrayType, StringType, IntegerType, DoubleType, LongType, FloatType
from pyspark.sql.types import *
from pyspark.sql.functions import date_format
from datetime import datetime
from pyspark.sql.functions import sum,trim,udf,lit
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

if __name__ == "__main__":

    import os
    import sys
    import shutil
    import subprocess
    db_certified = sys.argv[1]
    db_analytical_ds = sys.argv[2]
    db_analytical_temp = sys.argv[3]
    username= sys.argv[4]
    password= sys.argv[5]
    db= sys.argv[6]
    connection_str= sys.argv[7]
    source= sys.argv[8]
    group_id= sys.argv[9]
    table_name=sys.argv[10]
    cur_script= sys.argv[11]
    source_log_euw = sys.argv[12]
    euw_shell_script_path=sys.argv[13]
    current_dt = str(datetime.now().strftime("%Y%m%d"))
    log_dir = source_log_euw+"/"+current_dt+"/python_script_weather_logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    columns="inserted_count,total_count,table_name"

#**************************************************************************************************************************************************************

#### happusrd user development cluster

### creating spark-session
    spark = SparkSession.builder.appName("EUW_CUSTOMER_LOCATION_WEATHER_DATA").enableHiveSupport().getOrCreate()
    def fileLog(logMsg):
        with open(log_dir+"/EUW_CUSTOMER_LOCATION_WEATHER_DATA_LOG.log", "a") as myfile:
            myfile.write(str(logMsg)+ "\n")
    fileLog("################################## EUW_CUSTOMER_LOCATION_WEATHER_DATA script is started ###########################################")

### reading fixed flat actual data
    WEATHER_DAILY_FORECAST = spark.sql("select * from "+db_certified+".weather_zip_cd_daily_forecast")
    WEATHER_DAILY_ACTUALS = spark.sql("select zip_code,weather_concepts,weather_date,gmt,avg_daily_temp,min_daily_temp,max_daily_temp,updated_on,batch_id from "+db_certified+".weather_zip_cd_daily_actuals")
    Weather_all = WEATHER_DAILY_FORECAST.unionAll(WEATHER_DAILY_ACTUALS)
    fileLog("weather actuals and forecast have been unioned")
### Dcasting and casting weather_date as date

    df = Weather_all
    df = df.withColumn('temp_set',F.concat_ws(',',F.col('avg_daily_temp'),F.col('min_daily_temp'),F.col('max_daily_temp')))
    date_format_function =  udf (lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
    df = df.withColumn("weather_date",date_format_function(date_format(col("weather_date"),"yyyy-MM-dd")))

# applying pivot on weather concepts

    df1 = df.groupby('zip_code','weather_date').pivot('weather_concepts',['DBT','DPT','HUM']).agg(F.first('temp_set')).orderBy('zip_code','weather_date')

    split_col = F.split(df1['DBT'], ',')
    df1 = df1.withColumn('avg_daily_temp_DBT', split_col.getItem(0).cast(DoubleType()))
    df1 = df1.withColumn('min_daily_temp_DBT', split_col.getItem(1).cast(DoubleType()))
    df1 = df1.withColumn('max_daily_temp_DBT', split_col.getItem(2).cast(DoubleType()))


    split_col = F.split(df1['DPT'], ',')
    df1 = df1.withColumn('avg_daily_temp_DPT', split_col.getItem(0).cast(DoubleType()))
    df1 = df1.withColumn('min_daily_temp_DPT', split_col.getItem(1).cast(DoubleType()))
    df1 = df1.withColumn('max_daily_temp_DPT', split_col.getItem(2).cast(DoubleType()))

    split_col = F.split(df1['HUM'], ',')
    df1 = df1.withColumn('avg_daily_temp_HUM', split_col.getItem(0).cast(DoubleType()))
    df1 = df1.withColumn('min_daily_temp_HUM', split_col.getItem(1).cast(DoubleType()))
    df1 = df1.withColumn('max_daily_temp_HUM', split_col.getItem(2).cast(DoubleType()))

    df1 = df1.drop('DBT').drop('DPT').drop('HUM')
    fileLog("Dcasted the weather_concepts")
    Wthr_Dcast = df1.persist()
    Wthr_Dcast_Count=Wthr_Dcast.count()
    fileLog("Final counts :")
    fileLog(Wthr_Dcast_Count)
    Wthr_Dcast.createOrReplaceTempView("Wthr_Dcast")
    spark.sql("drop table if exists "+db_analytical_temp+".Euw_weather_data_temp")
    spark.sql("create table "+db_analytical_temp+".Euw_weather_data_temp as select * from Wthr_Dcast")
    column_values=[]
    column_values.insert(0,str(Wthr_Dcast_Count))
    column_values.append(str(Wthr_Dcast_Count))
    column_values.append('Euw_weather_data_temp')
    print(column_values)
    column_values=','.join(column_values).rstrip(',')
    print(column_values)
    path='/data01/data/dev/dif/files/scripts/euw'
    os.chdir(path)
    subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,table_name,cur_script,db_analytical_temp,columns,column_values)])

    fileLog("################################## EUW_CUSTOMER_LOCATION_WEATHER_DATA script is complete ###########################################")
