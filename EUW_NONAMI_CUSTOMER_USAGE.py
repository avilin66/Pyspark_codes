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
    conf = SparkConf().setAppName("NonAmi17prep")
    import os
    import sys
    import shutil
    import subprocess
    db_certified = sys.argv[1]
    db_analytical_ds = sys.argv[2]
    db_analytical_temp = sys.argv[3]
    dt = sys.argv[4]
    appYear=sys.argv[5]
    username= sys.argv[6]
    password= sys.argv[7]
    db= sys.argv[8]
    connection_str= sys.argv[9]
    source= sys.argv[10]
    group_id= sys.argv[11]
    cur_script1= sys.argv[12]
    cur_script2= sys.argv[13]
    source_log_euw= sys.argv[14]
    euw_shell_script_path=sys.argv[15]
    current_dt = str(dtm.now().strftime("%Y%m%d"))
    log_dir = source_log_euw+"/"+current_dt+"/python_script_logs"
    if not os.path.exists(log_dir):
       os.makedirs(log_dir)
    columns="inserted_count,total_count,table_name"


#**************************************************************************************************************************************************************

#### happusrd user development cluster

### creating spark-session
    spark = SparkSession.builder.appName("NonAmi17prep").enableHiveSupport().getOrCreate()

    def fileLog(logMsg):
        with open(log_dir+"/EUW_NONAMI_CUSTOMER_USAGE.log", "a") as myfile:
            myfile.write(str(logMsg)+ "\n")
    fileLog("################################## EUW_NONAMI_CUSTOMER_USAGE script is started ###########################################")
# prev and curr year
    prev_year=int(appYear)-1
    curr_year=int(appYear)

    fileLog("current year is : "+str(curr_year)+". Previous year is : "+str(prev_year))
## static codes required in the function
    usagev1 = spark.sql("select concat_agmnt_no,CUSTOMER_ID,ACCOUNT_SEQ,AGREEMENT_SEQ,SITE_ID,SERVICE_SEQ,SiteId_ServiceSeq,usage_value,usage_date,REGISTER_TYPE_CD from "+db_analytical_temp+".EUW_humidity_responsive_customers_Usage_temp")
    fileLog("humidity affected cutomer's usage has been read")
    def non_ami(year):
        myudf = lambda x: [i+1 for i in range(x)]
        label_udf = udf(myudf,ArrayType(IntegerType()))
        date_format_function =  udf (lambda x: dtm.strptime(x, '%Y-%m-%d'), DateType())

        non_ami = spark.sql("select SITE_ID,SERVICE_LOCATION_SEQ,SCHEDULE_MONTH,SCHEDULE_YEAR,USAGE_STATUS_CD,NET_USAGE_QUANTITY,METER_REGISTER_READING_TYPE_CD from "+db_certified+".meter_register_reading where trim(METER_REGISTER_TYPE_CD)='KWH'and trim(USAGE_STATUS_CD) in ('V') and trim(METER_REGISTER_READING_TYPE_CD) in ('2', '6') and delete_flag = ' ' and trim(SCHEDULE_YEAR)={}".format(year))
        fileLog("################### for year "+str(year)+" non_ami has been read ##########################")
        non_ami_count=non_ami.count()
        fileLog("################### for non_ami_count-->"+ str(non_ami_count))
        maxy_month = non_ami.select(max('SCHEDULE_MONTH')).persist().first()[0]
        fileLog("maximum month for non_ami : "+str(maxy_month))
        non_ami = non_ami.withColumn('SiteId_ServiceSeq',F.concat(F.col("SITE_ID"),F.col("SERVICE_LOCATION_SEQ")).cast(LongType()))
        non_ami = non_ami.withColumn('NET_USAGE_QUANTITY',F.col('NET_USAGE_QUANTITY').cast(DoubleType()))
        # take the records where non_ami17 and usagev1 has same siteid_serviceseq
        df1 = non_ami.alias('df1')
        df2 = usagev1.alias('df2').select('SiteId_ServiceSeq').distinct()
        non_ami = df1.join(df2,df1.SiteId_ServiceSeq==df2.SiteId_ServiceSeq).select('df1.*')
        # take the usage according to SiteId_ServiceSeq,SITE_ID,SERVICE_LOCATION_SEQ,SCHEDULE_MONTH,SCHEDULE_YEAR
        fileLog("aggregating the usage according to SiteId_ServiceSeq,SITE_ID,SERVICE_LOCATION_SEQ,SCHEDULE_MONTH,SCHEDULE_YEAR")
        MTR = non_ami.groupBy('SiteId_ServiceSeq','SITE_ID','SERVICE_LOCATION_SEQ','SCHEDULE_MONTH','SCHEDULE_YEAR').agg(sum(F.col('NET_USAGE_QUANTITY')).alias('USAGE'))
        # take unique set of site_id, service_seq and location_seq from mtr17
        # first for all the months in the year
        nonamiset = MTR.select('SiteId_ServiceSeq','SITE_ID','SERVICE_LOCATION_SEQ').distinct()
        fileLog("create a unique set of site_id,service_seq,location_seq")
        nonamiset = nonamiset.withColumn('SCHEDULE_MONTH',lit(maxy_month)).persist()
        temp = nonamiset.withColumn('SCHEDULE_MONTH_list',label_udf(F.col('SCHEDULE_MONTH')))
        nonami_all = temp.withColumn('SCHEDULE_MONTH', explode('SCHEDULE_MONTH_list'))
        nonami_all = nonami_all.orderBy('SiteId_ServiceSeq','SITE_ID')
        fileLog("now we have 12 records representing each month for every siteid_serviceseq")

        # check if the year is leap year
        if (( year%400 == 0) or (( year%4 == 0 ) and ( year%100 != 0))):
            fileLog(str(year)+" is a Leap Year. Hence number of days in feb month will be 29")
            nonami_all = nonami_all.withColumn("Daysinmon",F.when(F.col('SCHEDULE_MONTH') == 1, 31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 2, 29).otherwise(F.when(F.col('SCHEDULE_MONTH') == 3, 31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 4,30).otherwise(F.when(F.col('SCHEDULE_MONTH') == 5,31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 6,30).otherwise(F.when(F.col('SCHEDULE_MONTH') == 7, 31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 8,31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 9,30).otherwise(F.when(F.col('SCHEDULE_MONTH') == 10,31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 11,30).otherwise(F.when(F.col('SCHEDULE_MONTH') == 12,31).otherwise(0)))))))))))))
        else:
            fileLog(str(year)+" is not a Leap Year. Hence number of days in feb month will be 28")
            nonami_all = nonami_all.withColumn("Daysinmon",F.when(F.col('SCHEDULE_MONTH') == 1, 31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 2, 28).otherwise(F.when(F.col('SCHEDULE_MONTH') == 3, 31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 4,30).otherwise(F.when(F.col('SCHEDULE_MONTH') == 5,31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 6,30).otherwise(F.when(F.col('SCHEDULE_MONTH') == 7, 31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 8,31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 9,30).otherwise(F.when(F.col('SCHEDULE_MONTH') == 10,31).otherwise(F.when(F.col('SCHEDULE_MONTH') == 11,30).otherwise(F.when(F.col('SCHEDULE_MONTH') == 12,31).otherwise(0)))))))))))))
        # second for all the days lie in the months : means for 1'st jan til 31'st dec

        nonami_all = nonami_all.withColumn("SCHEDULE_YEAR",lit(year))
            # join the records with MTR17
        df1 = nonami_all.alias('df1')
        df2 = MTR.alias('df2')
        nonami_all = df1.join(df2,["SiteId_ServiceSeq","SITE_ID","SCHEDULE_MONTH","SERVICE_LOCATION_SEQ","SCHEDULE_YEAR"],"left_outer").select('df1.*',df2.USAGE)
        nonami_all = nonami_all.na.fill(0)
        fileLog("side ids having missing month have 0 usage values")
        # add the workaround (adding medians)
        i=1
        nonami_all = nonami_all.withColumn('Quantiled_usage',lit(0.0))
######### January ########
        if  i<=maxy_month :
            x1 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 1)).approxQuantile('USAGE',[0.5],0.0)[0]
            y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 1,x1).otherwise(F.col('Quantiled_usage')))
            i=i+1
######### February ########
            if i<=maxy_month :
                x2 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 2)).approxQuantile('USAGE',[0.5],0.0)[0]
                y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 2,x2).otherwise(F.col('Quantiled_usage')))
                i=i+1
######### March ########
                if i<=maxy_month :
                    x3 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 3)).approxQuantile('USAGE',[0.5],0.0)[0]
                    y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 3,x3).otherwise(F.col('Quantiled_usage')))
                    i=i+1
######### April ########
                    if i<=maxy_month :
                        x4 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 4)).approxQuantile('USAGE',[0.5],0.0)[0]
                        y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 4,x4).otherwise(F.col('Quantiled_usage')))
                        i=i+1
######### May ########
                        if i<=maxy_month :
                            x5 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 5)).approxQuantile('USAGE',[0.5],0.0)[0]
                            y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 5,x5).otherwise(F.col('Quantiled_usage')))
                            i=i+1
######### June ########
                            if i<=maxy_month :
                                x6 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 6)).approxQuantile('USAGE',[0.5],0.0)[0]
                                y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 6,x6).otherwise(F.col('Quantiled_usage')))
                                i=i+1
######### July ########
                                if i<=maxy_month :
                                    x7 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 7)).approxQuantile('USAGE',[0.5],0.0)[0]
                                    y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 7,x7).otherwise(F.col('Quantiled_usage')))
                                    i=i+1
######### August ########
                                    if i<=maxy_month :
                                        x8 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 8)).approxQuantile('USAGE',[0.5],0.0)[0]
                                        y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 8,x8).otherwise(F.col('Quantiled_usage')))
                                        i=i+1
######### September ########
                                        if i<=maxy_month :
                                            x9 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 9)).approxQuantile('USAGE',[0.5],0.0)[0]
                                            y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 9,x9).otherwise(F.col('Quantiled_usage')))
                                            i=i+1
######### October ########
                                            if i<=maxy_month :
                                                x10 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 10)).approxQuantile('USAGE',[0.5],0.0)[0]
                                                y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 10,x10).otherwise(F.col('Quantiled_usage')))
                                                i=i+1
######### November ########
                                                if i<=maxy_month :
                                                    x11 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 11)).approxQuantile('USAGE',[0.5],0.0)[0]
                                                    y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 11,x11).otherwise(F.col('Quantiled_usage')))
                                                    i=i+1
######### December ########
                                                    if i<=maxy_month :
                                                        x12 = statFunc(nonami_all.select('SiteId_ServiceSeq','SITE_ID','SCHEDULE_MONTH','SERVICE_LOCATION_SEQ','USAGE').where(F.col('SCHEDULE_MONTH') == 12)).approxQuantile('USAGE',[0.5],0.0)[0]
                                                        y = nonami_all.withColumn('Quantiled_usage',F.when(F.col('SCHEDULE_MONTH') == 12,x12).otherwise(F.col('Quantiled_usage')))
                                                        i=i+1


        fileLog("applying quantiled algorithms")
        fileLog("site_id's with missing months have quantiled usage of all the site_id for that month")

        nonami_all = y.withColumn('USAGE',F.when(F.col('USAGE') == 0.0,F.col('Quantiled_usage')).otherwise(F.col('USAGE')))
        # add daily_usage : average usage for every record present per month per site_id, service_seq
        fileLog("now for every unique site_id we have daily usage for every day till the last day of the year")
        nonami_all = nonami_all.withColumn('daily_usage',F.col('USAGE')/F.col('Daysinmon'))
# create a udf which can form a list of integer by taking daysincol like 31 - [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]
        temp = nonami_all.withColumn('Day',label_udf(F.col('Daysinmon')))
        temp = temp.orderBy('SiteId_ServiceSeq','SCHEDULE_MONTH')
        Mtr_Data = temp.withColumn('Day',explode(F.col('Day')))
        Mtr_Data = Mtr_Data.withColumn("merge", concat_ws("-", F.col("SCHEDULE_YEAR"), F.col("SCHEDULE_MONTH"), F.col("Day"))).withColumn("USAGE_DATE", F.col('merge')).drop(F.col("merge"))
        Mtr_Data = Mtr_Data.withColumn("USAGE_DATE",date_format_function(date_format(col("USAGE_DATE"),"yyyy-MM-dd")))

        if year==curr_year:
            Mtr_Data = Mtr_Data.filter(F.col('USAGE_DATE') < dt)
# tag agreement
        unique_set = usagev1.select('CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','SITE_ID','SERVICE_SEQ','SiteId_ServiceSeq').distinct()
        df1 = Mtr_Data.alias('df1')
        df2 = unique_set.alias('df2')
        MtratAgr = df1.join(df2,df1.SiteId_ServiceSeq==df2.SiteId_ServiceSeq).select('df1.*',df2.CUSTOMER_ID,df2.ACCOUNT_SEQ,df2.AGREEMENT_SEQ).drop(F.col('USAGE'))
        MtratAgr = MtratAgr.withColumnRenamed("daily_usage","USAGE_VALUE").withColumnRenamed("SERVICE_LOCATION_SEQ","SERVICE_SEQ").drop('SCHEDULE_MONTH_list')
        fileLog("################### for year "+str(year)+" non_ami is successfully complete ##########################")
        return MtratAgr


    df17 = non_ami(prev_year).persist()
    fileLog("datset for previous year has been read with counts :")
    df17_count=df17.count()
    fileLog(df17_count)
    df18 = non_ami(curr_year).persist()
    fileLog("datset for current year has been read with counts :")
    df18_count=df18.count()
    fileLog(df18_count)
    df17.createOrReplaceTempView("df17_temp")
    spark.sql("drop table if exists "+db_analytical_temp+".Euw_nonami_df17_temp")
    spark.sql("create table "+db_analytical_temp+".Euw_nonami_df17_temp as select * from df17_temp")
    column_values1=[]
    column_values1.insert(0,str(df17_count))
    column_values1.append(str(df17_count))
    column_values1.append('Euw_nonami_df17_temp')
    print(column_values1)
    column_values1=','.join(column_values1).rstrip(',')
    print(column_values1)
    df18.createOrReplaceTempView("df18_temp")
    spark.sql("drop table if exists "+db_analytical_temp+".Euw_nonami_df18_temp")
    spark.sql("create table "+db_analytical_temp+".Euw_nonami_df18_temp as select * from df18_temp")
    column_values2=[]
    column_values2.insert(0,str(df18_count))
    column_values2.append(str(df18_count))
    column_values2.append('Euw_nonami_df18_temp')
    print(column_values2)
    column_values2=','.join(column_values2).rstrip(',')
    subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,cur_script1,cur_script1,db_analytical_temp,columns,column_values1)])
    print("done for 17")
    subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,cur_script2,cur_script2,db_analytical_temp,columns,column_values2)])

    fileLog("################################## EUW_NONAMI_CUSTOMER_USAGE script is complete ###########################################")
