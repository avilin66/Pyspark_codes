from __future__ import print_function
from pyspark.sql import functions as F
from pyspark.sql.functions import mean, min, max, variance, lag, count, col
from pyspark import sql
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import ArrayType, StringType, IntegerType, DoubleType, LongType, FloatType
from pyspark.sql.types import *
from pyspark.sql.functions import date_format
from datetime import datetime as dtm
from pyspark.sql.functions import sum,trim,udf,lit,array,explode,concat_ws,month,concat,current_date
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameStatFunctions as statFunc


if __name__ == "__main__":

    import os
    import sys
    import subprocess
    db_certified = sys.argv[1]
    db_analytical_ds = sys.argv[2]
    db_analytical_temp = sys.argv[3]
    #dt=sys.argv[1]
    #appYear=sys.argv[2]
    username= sys.argv[4]
    password= sys.argv[5]
    db= sys.argv[6]
    connection_str= sys.argv[7]
    source= sys.argv[8]
    group_id= sys.argv[9]
    table_name= sys.argv[10]
    cur_script= sys.argv[11]
    source_log_euw=sys.argv[12]
    euw_shell_script_path=sys.argv[13]
    current_dt = str(dtm.now().strftime("%Y%m%d"))
    log_dir = source_log_euw+"/"+current_dt+"/python_script_logs"
    if not os.path.exists(log_dir):
       os.makedirs(log_dir)



#**************************************************************************************************************************************************************
    columns="inserted_count,total_count,table_name"

#### happusrd user development cluster

### creating spark-session
    spark = SparkSession.builder.appName("AMINonAmiprep").enableHiveSupport().getOrCreate()
    def fileLog(logMsg):
        with open(log_dir+"/EUW_AMI_AND_NONAMI_CUSTOMER_USAGE_LOG.log", "a") as myfile:
            myfile.write(str(logMsg)+ "\n")
    fileLog("################################## EUW_AMI_AND_NONAMI_CUSTOMER_USAGE script is started ###########################################")




########## new working code ###########################
    usagev1 = spark.sql("select concat_agmnt_no,CUSTOMER_ID,ACCOUNT_SEQ,AGREEMENT_SEQ,SITE_ID,SERVICE_SEQ,SiteId_ServiceSeq,usage_value,usage_date,REGISTER_TYPE_CD from  "+db_analytical_temp+".EUW_humidity_responsive_customers_Usage_temp")

    fileLog("weather responsive customers have been read")
    df17 = spark.sql("select * from  "+db_analytical_temp+".Euw_nonami_df17_temp")
    df18 = spark.sql("select * from  "+db_analytical_temp+".Euw_nonami_df18_temp")
    df1 = df18.alias('df1').withColumn('df_st',F.col('SiteId_ServiceSeq'))

    df2 = usagev1.withColumnRenamed('SiteId_ServiceSeq','usage_st').withColumnRenamed('usage_value','usage_usage_value').withColumnRenamed('CUSTOMER_ID','cust_id').withColumnRenamed('ACCOUNT_SEQ','acc_seq').withColumnRenamed('AGREEMENT_SEQ','agr_seq').select('usage_st','usage_usage_value','usage_date','cust_id','acc_seq','agr_seq').distinct()

    joined = df1.join(df2,[(df1.df_st==df2.usage_st) & (df1.USAGE_DATE==df2.usage_date) & (df1.CUSTOMER_ID==df2.cust_id) & (df1.ACCOUNT_SEQ==df2.acc_seq) & (df1.AGREEMENT_SEQ==df2.agr_seq)],"left_outer").select('df1.*',df2.usage_usage_value)
    joined = joined.withColumn('final_usage_value',F.when(F.col('usage_usage_value').isNotNull(),F.col('usage_usage_value')).otherwise(F.col('USAGE_VALUE')))
    Siteid_serseq_list = usagev1.select("siteid_serviceseq").distinct().rdd.flatMap(lambda x: x).collect()
    i = [long(a) for a in Siteid_serseq_list]


    mtr_usg18 = joined.filter(F.col('SiteId_ServiceSeq').isin(i))
    mtr_usg18 = mtr_usg18.select('CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','USAGE_DATE','SiteId_ServiceSeq','SITE_ID','SERVICE_SEQ','SCHEDULE_MONTH','SCHEDULE_YEAR','Daysinmon','Day','final_usage_value')
    mtr_usg18 = mtr_usg18.withColumnRenamed('final_usage_value','USAGE_VALUE')
    fileLog("nonami customers who have siteid_serviceseq matching with usage is stored")

    df18_siteid_list = df18.select("siteid_serviceseq").distinct().rdd.flatMap(lambda x: x).collect()
    i = [long(a) for a in df18_siteid_list]
    Usage_nonmatch18 = usagev1.filter(F.col('SiteId_ServiceSeq').isin(i) == False)
    fileLog("customers who are only there in usage but not in ami meter usage is stored ")

# get matching ids for 2017 data also
    df1 = mtr_usg18.select('CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','SITE_ID','SERVICE_SEQ','USAGE_DATE','USAGE_VALUE')
    df2 = Usage_nonmatch18.select('CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','SITE_ID','SERVICE_SEQ','USAGE_DATE','USAGE_VALUE')
    match_nonMatch18 = df1.union(df2)
    ## join ami, non_ami of 2017
    df1 = df17.select('CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','SITE_ID','SERVICE_SEQ','USAGE_DATE','USAGE_VALUE')
    df2 = match_nonMatch18.alias('df2').select('CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','SITE_ID','SERVICE_SEQ','USAGE_DATE','USAGE_VALUE')
    nonami_ami = df1.union(df2)
    # add concat_aggment_number and order
    nonami_ami = nonami_ami.withColumn('CONCAT_AGMNT_NO',F.concat(F.col('CUSTOMER_ID'),F.col('ACCOUNT_SEQ'),F.col('AGREEMENT_SEQ')).cast(LongType()))
    nonami_ami = nonami_ami.orderBy('CONCAT_AGMNT_NO','USAGE_DATE').persist()
    fileLog("Total number of aminonami customers are : ")
    nonami_ami_count=nonami_ami.count()
    fileLog(nonami_ami_count)

    nonami_ami.createOrReplaceTempView("AMInonAMI")
    spark.sql("drop table if exists "+db_analytical_temp+".Euw_ami_nonami_temp")
    spark.sql("create table "+db_analytical_temp+".Euw_ami_nonami_temp as select * from AMInonAMI")
    column_values=[]
    column_values.insert(0,str(nonami_ami_count))
    column_values.append(str(nonami_ami_count))
    column_values.append('Euw_ami_nonami_temp')
    print(column_values)
    column_values=','.join(column_values).rstrip(',')
    print(column_values)
    #path='/data01/data/dev/dif/files/scripts/euw'
    #os.chdir(path)
    subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,table_name,cur_script,db_analytical_temp,columns,column_values)])

    fileLog("################################## EUW_AMI_AND_NONAMI_CUSTOMER_USAGE script is complete ###########################################")

