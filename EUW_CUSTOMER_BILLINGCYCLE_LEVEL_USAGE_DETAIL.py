from __future__ import print_function
from pyspark.sql import functions as F
from pyspark.sql.functions import mean, min, max, variance, lag, count, col, unix_timestamp
from pyspark import sql
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import ArrayType, StringType, IntegerType, DoubleType, LongType, FloatType
from pyspark.sql.types import *
from pyspark.sql.functions import date_format,date_sub,date_add
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
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

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
    table_name= sys.argv[10]
    cur_script=sys.argv[11]
    source_log_euw= sys.argv[12]
    euw_shell_script_path=sys.argv[13]
    dt=sys.argv[14]
    current_dt = str(dtm.now().strftime("%Y%m%d"))
    log_dir = source_log_euw+"/"+current_dt+"/python_script_logs"

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    columns="inserted_count,total_count,table_name"



#**************************************************************************************************************************************************************

#### happusrd user development cluster

### creating spark-session
    spark = SparkSession.builder.appName("EUW_CUSTOMER_BILLINGCYCLE_LEVEL_USAGE_DETAIL").enableHiveSupport().getOrCreate()
    def fileLog(logMsg):
        with open(log_dir+"/EUW_CUSTOMER_BILLINGCYCLE_LEVEL_USAGE_DETAIL_LOG.log", "a") as myfile:
            myfile.write(str(logMsg)+ "\n")
    fileLog("################################## EUW_CUSTOMER_BILLINGCYCLE_LEVEL_USAGE_DETAIL script is started ###########################################")

# read again to load the imputed dataset  : make it as a different script

    usg_bc = spark.sql("select * from "+db_analytical_temp+".Euw_customer_nonami_bc_imputed_temp")
# casting billing_start_date and billing_end_date as date
    date_format_function =  udf (lambda x: dtm.strptime(x, '%Y-%m-%d'), DateType())
    usg_bc = usg_bc.withColumn("billing_start_date",date_format_function(date_format(col("billing_start_date"),"yyyy-MM-dd")))
    usg_bc = usg_bc.withColumn("billing_end_date",date_format_function(date_format(col("billing_end_date"),"yyyy-MM-dd")))
    fileLog("imputed data from hive is read")
# add HDD/CDD
    usg_bc = usg_bc.withColumn('HDD',F.col('max_HDD')+F.col('min_HDD'))
    usg_bc = usg_bc.withColumn('CDD',F.col('max_CDD')+F.col('min_CDD'))

    usg_bc = usg_bc.orderBy('CONCAT_AGMNT_NO','Billing_Start_Date')
    usg_bc = usg_bc.withColumn('BC_USG',F.when(F.col('Billing_Cycle') == 'BC_14',lit(0)).otherwise(F.col('BC_USG')))
    usg_bc = usg_bc.withColumn('Weekend_USG',F.when(F.col('Billing_Cycle') == 'BC_14',lit(0)).otherwise(F.col('Weekend_USG')))
    usg_bc = usg_bc.withColumn('Weekdays_USG',F.when(F.col('Billing_Cycle') == 'BC_14',lit(0)).otherwise(F.col('Weekdays_USG')))

    usg_bc = usg_bc.withColumn('Seasonal_Usg17',F.when((F.col('Summer_Days') >0) & (F.col('Fall_Days') > 0),((F.col('Summer_Days')*F.col('Avg_SummerUsg'))+(F.col('Fall_Days')*F.col('Avg_FallUsg')))/30).otherwise(
    F.when((F.col('Fall_Days') >0) & (F.col('Winter_Days') > 0),((F.col('Fall_Days')*F.col('Avg_FallUsg'))+(F.col('Winter_Days')*F.col('Avg_WinterUsg')))/30).otherwise(
    F.when((F.col('Winter_Days') >0) & (F.col('Spring_Days') > 0),((F.col('Winter_Days')*F.col('Avg_WinterUsg'))+(F.col('Spring_Days')*F.col('Avg_SpringUsg')))/30).otherwise(
    F.when((F.col('Spring_Days') >0) & (F.col('Summer_Days') > 0),((F.col('Spring_Days')*F.col('Avg_SpringUsg'))+(F.col('Summer_Days')*F.col('Avg_SummerUsg')))/30).otherwise(lit(0.0)
    )))))

    usg_bc = usg_bc.select('*').where(F.col('Seasonal_Usg17').isNotNull())
    usg_bc = usg_bc.withColumn('Seasonal_Usg17_new',F.when((F.col('Seasonal_Usg17') == 0) & (F.col('Summer_Days') > 0),F.col('Avg_SummerUsg')).otherwise(
    F.when((F.col('Seasonal_Usg17')== 0) & (F.col('Winter_Days') > 0),F.col('Avg_WinterUsg')).otherwise(
    F.when((F.col('Seasonal_Usg17')== 0) & (F.col('Spring_Days') > 0),F.col('Avg_SpringUsg')).otherwise(
    F.when((F.col('Seasonal_Usg17')== 0) & (F.col('Fall_Days') > 0),F.col('Avg_FallUsg')).otherwise(F.col('Seasonal_Usg17')
    )))))
    usg_bc = usg_bc.drop('Seasonal_Usg17').withColumnRenamed('Seasonal_Usg17_new','Seasonal_Usg17')
    # lag variables
    ww = Window.partitionBy('CONCAT_AGMNT_NO').orderBy('Billing_Start_Date')

    usg_bc = usg_bc.withColumn('Last1BC_USG',lag(usg_bc['BC_USG']).over(ww))
    usg_bc = usg_bc.withColumn('Last2BC_USG',lag(usg_bc['Last1BC_USG']).over(ww))
    usg_bc = usg_bc.withColumn('SuccUsgDiff',F.col('BC_USG')-F.col('Last1BC_USG'))
    usg_bc = usg_bc.withColumn('LagSuccUsgDiff',lag(usg_bc['SuccUsgDiff']).over(ww))
    usg_bc = usg_bc.withColumn('LagWeekend_USG',lag(usg_bc['Weekend_USG']).over(ww))
    usg_bc = usg_bc.withColumn('LagWeekdays_USG',lag(usg_bc['Weekdays_USG']).over(ww))
    usg_bc = usg_bc.withColumn('LagBC_AVGDPT',lag(usg_bc['BC_AVGDPT']).over(ww))
    usg_bc = usg_bc.withColumn('LagBC_AVGHUM',lag(usg_bc['BC_AVGHUM']).over(ww))
    usg_bc = usg_bc.withColumn('LagBC_AVGTEMP',lag(usg_bc['BC_AVGTEMP']).over(ww))
    usg_bc = usg_bc.withColumn('StempDiffDPT',(usg_bc['BC_AVGDPT']-usg_bc['LagBC_AVGDPT']))
    usg_bc = usg_bc.withColumn('StempDiffHUM',(usg_bc['BC_AVGHUM']-usg_bc['LagBC_AVGHUM']))
    usg_bc = usg_bc.withColumn('StempDiff',(usg_bc['BC_AVGTEMP']-usg_bc['LagBC_AVGTEMP']))
    usg_bc = usg_bc.fillna(0)
    usg_bc = usg_bc.withColumn('AvgUsg_Last2',(usg_bc['Last1BC_USG']+usg_bc['Last2BC_USG'])/2)
    #usg_bc = usg_bc.withColumn('run_date',lit(dtm.now().strftime("%Y-%m-%d")))
    usg_bc = usg_bc.withColumn('run_date',lit(dt))
    usg_bc = usg_bc.withColumn('run_date',date_format_function(date_format(col("run_date"),"yyyy-MM-dd")))
    usg_bc = usg_bc.filter(F.col('billing_cycle') != 'BC_13').persist()
    usg_bc_count=usg_bc.count()
    fileLog("Final counts for usg_bc is : ")
    fileLog(usg_bc_count)



    usg_bc.createOrReplaceTempView("usg_bc_temp")
    spark.sql("drop table if exists "+db_analytical_ds+".Euw_customer_billing_cycle_level_usage")
    spark.sql("create table "+db_analytical_ds+".Euw_customer_billing_cycle_level_usage as select * from usg_bc_temp")
    column_values=[]
    column_values.insert(0,str(usg_bc_count))
    column_values.append(str(usg_bc_count))
    column_values.append('Euw_customer_billing_cycle_level_usage')
    print(column_values)
    column_values=','.join(column_values).rstrip(',')
    print(column_values)
    subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,table_name,cur_script,db_analytical_ds,columns,column_values)])

    fileLog("################################## EUW_CUSTOMER_BILLINGCYCLE_LEVEL_USAGE_DETAIL script is complete ###########################################")
