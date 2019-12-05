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

if __name__ == "__main__":
    import os
    import sys
    import shutil
    import subprocess
    db_certified = sys.argv[1]
    db_analytical_ds = sys.argv[2]
    db_analytical_temp = sys.argv[3]
    dt = sys.argv[4]
    username= sys.argv[5]
    password= sys.argv[6]
    db= sys.argv[7]
    connection_str= sys.argv[8]
    source= sys.argv[9]
    group_id= sys.argv[10]
    table_name= sys.argv[11]
    cur_script= sys.argv[12]
    source_log_euw=sys.argv[13]
    euw_shell_script_path=sys.argv[14]
    current_dt = str(datetime.now().strftime("%Y%m%d"))
    log_dir = source_log_euw+"/"+current_dt+"/python_script_logs"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.makedirs(log_dir)
    dayOfMonth= dt[5:7]+dt[8:10]

#**************************************************************************************************************************************************************

#### happusrd user development cluster
### creating spark-session
    spark = SparkSession.builder.appName("EUW_WEATHER_RESPONSIVE_CUSTOMER").enableHiveSupport().getOrCreate()
    columns="inserted_count,total_count,table_name"

    #def fileLog(logMsg):
        #with open(log_dir+"/EUW_WEATHER_RESPONSIVE_CUSTOMER.log", "a") as myfile:
            #myfile.write(str(logMsg)+ "\n")
### reading daily usage from "+db_certified+" layer
    customer_df = spark.sql("select customer_id from "+db_certified+".customer where trim(customer_type_cd)='R'")
##    fileLog("################################## weather_response script is started ###########################################")
##    fileLog("customer dataset has been read")
### reading fixed flat file
#    fileLog("Reading the dataset which contain customers getting affected by humidity")
    valid_customers_all = spark.sql("select * from "+db_analytical_ds+".Euw_valid_customers").filter(F.col('CONCAT_AGMNT_NO') != 'CONCAT_AGMNT_NO')
#    fileLog("######### Valid customers have been read #########")
    valid_customers = valid_customers_all.select("CONCAT_AGMNT_NO",'CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ')
    valid_customers = valid_customers.withColumn("CONCAT_AGMNT_NO",F.col('CONCAT_AGMNT_NO').cast(LongType())).withColumn('CUSTOMER_ID',F.col('CUSTOMER_ID').cast(IntegerType())).withColumn('ACCOUNT_SEQ',F.col('ACCOUNT_SEQ').cast(IntegerType())).withColumn('AGREEMENT_SEQ',F.col('AGREEMENT_SEQ').cast(IntegerType())).distinct()
# take out the customers who are affected by change in the temperature/humidity
    df1 = valid_customers.alias("df1")
    df2 = customer_df.alias("df2")
    customer_humidity = df1.join(df2,df1.CUSTOMER_ID==df2.customer_id).select("df1.*")

    Usage = spark.sql("select usage_date,customer_id,account_seq,agreement_seq,site_id,service_seq,REGISTER_TYPE_CD,Usage_value from "+db_certified+".daily_usage where trim(REGISTER_TYPE_CD)='KWH' and status='Complete' and USAGE_DATE < '"+dt+"'")
    Usage = Usage.withColumn('CONCAT_AGMNT_NO',F.concat(F.col('CUSTOMER_ID'),F.col('ACCOUNT_SEQ'),F.col('AGREEMENT_SEQ')).cast(LongType()))
    # converting into date type
    date_format_function =  udf (lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
    Usage = Usage.withColumn("USAGE_DATE",date_format_function(date_format(col("USAGE_DATE"),"YYYY-MM-dd")))
    # adding site_id_serviceseq
    Usage = Usage.withColumn('SiteId_ServiceSeq',F.concat(F.col('SITE_ID'),F.col('SERVICE_SEQ')).cast(LongType()))
    Usage = Usage.withColumn('usage_value',F.col('usage_value').cast(DoubleType()))
# getting usage of all the customers
#    fileLog("getting usage of all the customers with counts:")
    df1 = Usage.alias("df1")
    df2 = customer_humidity.alias("df2")
    Usagev1 = df1.join(df2,"CONCAT_AGMNT_NO").select("df1.*")
    #Usagev1_Count=Usagev1.count()
#    fileLog(Usagev1_Count)
    
    Usagev1.createOrReplaceTempView("Usagev1_temp")
    spark.sql("drop table if exists "+db_analytical_temp+".EUW_humidity_responsive_customers_Usage_temp_"+dayOfMonth)
    spark.sql("create table "+db_analytical_temp+".EUW_humidity_responsive_customers_Usage_temp_"+dayOfMonth+" as select * from Usagev1_temp")
    #column_values=[]
    ##column_values.insert(0,str(Usagev1_Count))
    ##column_values.append(str(Usagev1_Count))
    #column_values.append('EUW_humidity_responsive_customers_Usage_temp')
    #print(column_values)
    #column_values=','.join(column_values).rstrip(',')
    #print(column_values)
    #path='/data01/data/dev/dif/files/scripts/euw'  
    #os.chdir(path)
    #subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,table_name,cur_script,db_analytical_temp,columns,column_values)])
#    fileLog("################################## weather_response script is complete successfully ###########################################")