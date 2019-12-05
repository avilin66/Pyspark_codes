from __future__ import print_function
from pyspark.sql import functions as F
from pyspark.sql.functions import mean, min, max, variance, lag, count, col, unix_timestamp
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
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from datetime import date, timedelta
from datetime import datetime as dtm
import datetime
import calendar
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import DateType

if __name__ == "__main__":

    import os
    import sys
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
    source_log_euw= sys.argv[12]
    euw_shell_script_path=sys.argv[13]
    appYear=sys.argv[14]
    current_dt = str(dtm.now().strftime("%Y%m%d"))
    log_dir = source_log_euw+"/"+current_dt+"/python_script_logs"
    if not os.path.exists(log_dir):
       os.makedirs(log_dir)
    columns="inserted_count,total_count,table_name"

#**************************************************************************************************************************************************************

#### happusrd user development cluster

# prev and curr year
    prev_year=int(appYear)-1
    curr_year=int(appYear)
### creating spark-session
    spark = SparkSession.builder.appName("EUW_CUSTOMER_USAGE_HOLIDAY_DETAIL").enableHiveSupport().getOrCreate()
    def fileLog(logMsg):
        with open(log_dir+"/EUW_CUSTOMER_USAGE_HOLIDAY_DETAIL_LOG.log", "a") as myfile:
            myfile.write(str(logMsg)+ "\n")
    fileLog("################################## EUW_CUSTOMER_USAGE_HOLIDAY_DETAIL script is started ###########################################")

### reading AMInonAMI_zip data
    AMInonAMI_Zip = spark.sql("select a.CONCAT_AGMNT_NO,a.CUSTOMER_ID,a.ACCOUNT_SEQ, a.AGREEMENT_SEQ, a.SITE_ID,a.SERVICE_SEQ, a.USAGE_DATE, a.USAGE_VALUE,  b.town_code from "+db_analytical_temp+".Euw_ami_nonami_temp a inner join "+db_analytical_temp+".Euw_aggregated_cust_zip_temp b on  a.CONCAT_AGMNT_NO==b.concat_agmnt_no and a.CUSTOMER_ID==b.customer_id and a.ACCOUNT_SEQ==b.account_seq and a.AGREEMENT_SEQ==b.agreement_seq")
    AMInonAMI_Zip = AMInonAMI_Zip.withColumnRenamed("TOWN_CODE", "ZIP_CODE")
    AMInonAMI_Zip = AMInonAMI_Zip.withColumn('USAGE_VALUE',F.col('USAGE_VALUE').cast(DoubleType()))
    fileLog("reading AMInonAMI_Zip data")
### reading weather_dcast data
    Wthr_Dcast = spark.sql("select * from "+db_analytical_temp+".Euw_weather_data_temp")
    date_format_function =  udf (lambda x: dtm.strptime(x, '%Y-%m-%d'), DateType())
    Wthr_Dcast = Wthr_Dcast.withColumn("WEATHER_DATE",date_format_function(date_format(col("WEATHER_DATE"),"yyyy-MM-dd")))
    fileLog("reading weather_dcast data")
# take the unique set out of it
    usage_set = AMInonAMI_Zip.select('CONCAT_AGMNT_NO','CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','SITE_ID','SERVICE_SEQ','ZIP_CODE').distinct()
##populate additional future dates wrt usage_date and weather dates ###
    fileLog("reading unique set of agreements and populating the future dates wrt usage_date and weather dates")
    last_usage_date = AMInonAMI_Zip.agg(max('USAGE_DATE').alias('max_usage')).first()[0]#+timedelta(1)
    last_weather_date = Wthr_Dcast.agg(max('WEATHER_DATE').alias('max_weather')).first()[0]

    my_udf = lambda domain: [last_usage_date + timedelta(1) + timedelta(days=x) for x in range((last_weather_date-last_usage_date).days)]
    label_udf = udf(my_udf, ArrayType(DateType()))
    usage_set = usage_set.withColumn("USAGE_DATE",label_udf(F.col('ACCOUNT_SEQ')))
    usage_testv1 = usage_set.withColumn('USAGE_DATE', explode('USAGE_DATE'))
    usage_testv1 = usage_testv1.orderBy('CONCAT_AGMNT_NO','CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ')
    fileLog("population is complete")
# rbind with aminonami
    aminonami_zip_set = AMInonAMI_Zip.select('CONCAT_AGMNT_NO','CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','SITE_ID','SERVICE_SEQ','ZIP_CODE','USAGE_DATE')
    Usage_zipv1 = aminonami_zip_set.union(usage_testv1)
    fileLog("unioning it with existing usage data")
# workaround
    #Usage_zipv1 = aminonami_zip_set
# merge aminonami_usage_zip with weather
    df1 = Usage_zipv1.alias('df1')
    df2 = Wthr_Dcast.alias('df2').withColumnRenamed('zip_code','Wthr_zip_code')
    Usage_zipv2 = df1.join(df2,[(df1.ZIP_CODE==df2.Wthr_zip_code) & (df1.USAGE_DATE==df2.WEATHER_DATE)])
    fileLog("merge aminonami_usage_zip with weather")
##merging Usage_value######
    df1 = Usage_zipv2.alias('df1')
    df2 = AMInonAMI_Zip.alias('df2')
    Usage_zipv3 = df1.join(df2,['CONCAT_AGMNT_NO','CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','SITE_ID','SERVICE_SEQ','USAGE_DATE'],"left_outer").select(df1.CONCAT_AGMNT_NO,df1.CUSTOMER_ID,df1.ACCOUNT_SEQ,df1.AGREEMENT_SEQ,df1.SITE_ID,df1.SERVICE_SEQ,df1.USAGE_DATE,df1.avg_daily_temp_DBT,df1.avg_daily_temp_DPT,df1.avg_daily_temp_HUM,df1.min_daily_temp_DBT,df1.min_daily_temp_DPT,df1.min_daily_temp_HUM,df1.max_daily_temp_DBT,df1.max_daily_temp_DPT,df1.max_daily_temp_HUM,df1.ZIP_CODE,df2.USAGE_VALUE)
    Usage_zipv3 = Usage_zipv3.orderBy('CONCAT_AGMNT_NO','CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','USAGE_DATE')

## Reading holiday data
    #schema = StructType([StructField("Year", IntegerType(),True),StructField("Month", IntegerType(),True),StructField("Day", IntegerType(),True),StructField("Holiday_Ind", IntegerType(),True),StructField("Dates", StringType(),True)])
    #Holiday17 = spark.sql("select * from "+db_analytical_ds+".Euw_holidays_2017").filter(F.col('Day').isNotNull())
    #Holiday18 = spark.sql("select * from "+db_analytical_ds+".Euw_holidays_2018").filter(F.col('Day').isNotNull())
    #Holiday = Holiday17.union(Holiday18)
    Holiday = spark.sql("select * from "+db_analytical_ds+".Euw_holidays").filter(F.col('Day').isNotNull())
    fileLog("Reading holiday data")
    date_format_function = udf (lambda x: dtm.strptime(x, '%m/%d/%Y'), DateType())
    Holiday = Holiday.withColumn("Dates_formatted", (date_format_function(col('Dates'))))
# calculate day of week
    weekday_list = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_of_week_number = udf(lambda x,y,z: calendar.weekday(x, y, z),IntegerType())
    day_of_week = udf(lambda x: weekday_list[x],StringType())
    Holiday = Holiday.withColumn("DAYOFWKNO",day_of_week_number(col("Year"),col("Month"),col("Day")))
    Holiday = Holiday.withColumn("DayofWk",day_of_week(Holiday.DAYOFWKNO))


# join holiday data with usage_zipv3
    df1 = Usage_zipv3.alias('df1')
    df2 = Holiday.alias('df2')
    UsgWthr_hldy = df1.join(df2,df1.USAGE_DATE==df2.Dates_formatted)
    UsgWthr_hldy = UsgWthr_hldy.withColumnRenamed('avg_daily_temp_DBT','AVG_DBT').withColumnRenamed('min_daily_temp_DBT','MIN_DBT').withColumnRenamed('max_daily_temp_DBT','MAX_DBT')
    UsgWthr_hldy = UsgWthr_hldy.withColumnRenamed('avg_daily_temp_DPT','AVG_DPT').withColumnRenamed('min_daily_temp_DPT','MIN_DPT').withColumnRenamed('max_daily_temp_DPT','MAX_DPT')
    UsgWthr_hldy = UsgWthr_hldy.withColumnRenamed('avg_daily_temp_HUM','AVG_HUM').withColumnRenamed('min_daily_temp_HUM','MIN_HUM').withColumnRenamed('max_daily_temp_HUM','MAX_HUM')
    fileLog("join holiday data with usage_zipv3")
# adding derived variables
    UsgWthr_hldy = UsgWthr_hldy.withColumn('Summer_Ind',F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,06,01),datetime.date(prev_year,8,31))),1).otherwise(F.when((F.col('USAGE_DATE').between(datetime.date(curr_year,06,01),datetime.date(curr_year,8,31))),1).otherwise(0)))
    UsgWthr_hldy = UsgWthr_hldy.withColumn('Spring_Ind',F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,03,01),datetime.date(prev_year,05,31))),1).otherwise(F.when((F.col('USAGE_DATE').between(datetime.date(curr_year,03,01),datetime.date(curr_year,05,31))),1).otherwise(0)))
    UsgWthr_hldy = UsgWthr_hldy.withColumn('Fall_Ind',F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,9,01),datetime.date(prev_year,11,30))),1).otherwise(F.when((F.col('USAGE_DATE').between(datetime.date(curr_year,9,01),datetime.date(curr_year,11,30))),1).otherwise(0)))
# check if the prev and current year is leap year
# check if the prev and current year is leap year
    if (( prev_year%400 == 0) or (( prev_year%4 == 0 ) and ( prev_year%100 != 0))):
        fileLog(str(prev_year)+" is a Leap Year. Hence number of days in feb month will be 29" and str(curr_year)+ "is not a leap year. Hence number of days in feb will be 28")
        UsgWthr_hldy = UsgWthr_hldy.withColumn('Winter_Ind',F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,12,01),datetime.date(prev_year,12,31))),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(prev_year,01,01),datetime.date(prev_year,02,29)),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(curr_year,12,01),datetime.date(curr_year,12,31)),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(curr_year,01,01),datetime.date(curr_year,02,28)),1).otherwise(0)))))
    elif (( curr_year%400 == 0) or (( curr_year%4 == 0 ) and ( curr_year%100 != 0))):
        fileLog(str(curr_year)+" is a Leap Year. Hence number of days in feb month will be 29" and str(prev_year)+ "is not a leap year. Hence number of days in feb will be 28")
        UsgWthr_hldy = UsgWthr_hldy.withColumn('Winter_Ind',F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,12,01),datetime.date(prev_year,12,31))),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(prev_year,01,01),datetime.date(prev_year,02,28)),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(curr_year,12,01),datetime.date(curr_year,12,31)),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(curr_year,01,01),datetime.date(curr_year,02,29)),1).otherwise(0)))))
    else :
        fileLog(str(curr_year)+" and "+str(prev_year)+" are not Leap Years. Hence number of days in feb month will be 28 for both the years")
        UsgWthr_hldy = UsgWthr_hldy.withColumn('Winter_Ind',F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,12,01),datetime.date(prev_year,12,31))),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(prev_year,01,01),datetime.date(prev_year,02,28)),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(curr_year,12,01),datetime.date(curr_year,12,31)),1).otherwise(F.when(F.col('USAGE_DATE').between(datetime.date(curr_year,01,01),datetime.date(curr_year,02,28)),1).otherwise(0)))))

    UsgWthr_hldy = UsgWthr_hldy.withColumn('TempTransitionHflag',F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,04,01),datetime.date(prev_year,05,31))),1).otherwise(F.when((F.col('USAGE_DATE').between(datetime.date(curr_year,04,01),datetime.date(curr_year,05,31))),1).otherwise(0)))
    UsgWthr_hldy = UsgWthr_hldy.withColumn('TempTransitionLflag',F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,9,01),datetime.date(prev_year,9,30))),1).otherwise(F.when((F.col('USAGE_DATE').between(datetime.date(prev_year,10,01),datetime.date(prev_year,10,31))),1).otherwise(0)))
    UsgWthr_hldy = UsgWthr_hldy.persist()
    fileLog("Usage with zip and holiday joined with counts : ")
    UsgWthr_hldy_count=UsgWthr_hldy.count()
    fileLog(UsgWthr_hldy_count)
# store in hive

    UsgWthr_hldy.createOrReplaceTempView("UsgWthr_hldy_temp")
    spark.sql("drop table if exists  "+db_analytical_temp+".Euw_customer_aminonami_holiday_usage_temp")
    spark.sql("create table "+db_analytical_temp+".Euw_customer_aminonami_holiday_usage_temp as select * from UsgWthr_hldy_temp")
    column_values=[]
    column_values.insert(0,str(UsgWthr_hldy_count))
    column_values.append(str(UsgWthr_hldy_count))
    column_values.append('Euw_customer_aminonami_holiday_usage_temp')
    print(column_values)
    column_values=','.join(column_values).rstrip(',')
    print(column_values)
    #path='/data01/data/dev/dif/files/scripts/euw'
    #os.chdir(path)
    subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,table_name,cur_script,db_analytical_temp,columns,column_values)])


    fileLog("################################## EUW_CUSTOMER_USAGE_HOLIDAY_DETAIL script is complete ###########################################")
