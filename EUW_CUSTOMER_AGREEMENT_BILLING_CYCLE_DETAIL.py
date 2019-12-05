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
    dt=sys.argv[4]
    username= sys.argv[5]
    password= sys.argv[6]
    db= sys.argv[7]
    connection_str= sys.argv[8]
    source= sys.argv[9]
    group_id= sys.argv[10]
    table_name= sys.argv[11]
    cur_script=sys.argv[12]
    source_log_euw= sys.argv[13]
    euw_shell_script_path=sys.argv[14]
    current_dt = str(dtm.now().strftime("%Y%m%d"))
    log_dir = source_log_euw+"/"+current_dt+"/python_script_logs"
    if not os.path.exists(log_dir):
       os.makedirs(log_dir)
    columns="inserted_count,total_count,table_name"


#**************************************************************************************************************************************************************

#### happusrd user development cluster

### creating spark-session
    spark = SparkSession.builder.appName("EUW_CUSTOMER_AGREEMENT_BILLING_CYCLE_DETAIL").enableHiveSupport().getOrCreate()
    def fileLog(logMsg):
        with open(log_dir+"/EUW_CUSTOMER_AGREEMENT_BILLING_CYCLE_DETAIL_LOG.log", "a") as myfile:
            myfile.write(str(logMsg)+ "\n")
    fileLog("################################## EUW_CUSTOMER_AGREEMENT_BILLING_CYCLE_DETAIL script is started ###########################################")

### reading AMInonAMI_zip data


# Replace AGRMT_INVOICE_ACTIVITY with Customer_Agreement and Mete_Register_Reading
    bc = spark.sql("select distinct ca.customer_ID, ca.customer_account_sequence as account_seq, ca.customer_agreement_sequence as agreement_seq, mrr.meter_register_reading_dt as meter_reading_date from  "+db_certified+".customer_agreement ca join "+db_certified+".meter_register_reading mrr on ca.SITE_ID_PURCH = mrr.SITE_ID and ca.SERVICE_LOC_SEQ_PURCH = mrr.SERVICE_LOCATION_SEQ where ca.active_flag='Y' and ca.status_cd IN ('ACT','OPN','TFB') and mrr.meter_register_reading_dt < to_date('"+dt+"')")

    fileLog("bc data has been read successfully")
    bc = bc.orderBy('customer_ID','meter_reading_date')
# discarding agreements who have only 1 meter_reading_date : Also combining the step to find max reading date per concat_agreement
    bc = bc.withColumn('CONCAT_AGMNT_NO',F.concat(F.col('CUSTOMER_ID'),F.col('ACCOUNT_SEQ'),F.col('AGREEMENT_SEQ')).cast(LongType()))
    grouped = bc.groupBy('CONCAT_AGMNT_NO').agg(count('meter_reading_date').alias('agr_count'),max('meter_reading_date').alias('Billing1_End_Date')).where(F.col('agr_count') > 1)
# join it with bc to get full column details
    df1 = bc.alias('df1')
    df2 = grouped.alias('df2')
    bc_req = df1.join(df2,['CONCAT_AGMNT_NO']).select('df1.*',df2.Billing1_End_Date)
    billingcycle = bc_req
    fileLog("counts of unique aggreements in billing cycle before joining are: "+str(billingcycle.select('CONCAT_AGMNT_NO').distinct().count()))

# get max date for every agreement
    billingcycle = bc_req.groupBy('CONCAT_AGMNT_NO','customer_id','account_seq','agreement_seq').agg(max('meter_reading_date').alias('Billing1_End_Date'))
# generate last 13 billing cycles

    billingcycle = billingcycle.withColumn('Billing1_Start_Date',date_sub(F.col('Billing1_End_Date'),29))
    billingcycle = billingcycle.withColumn('Billing2_End_Date',date_sub(F.col('Billing1_Start_Date'),1))
    billingcycle = billingcycle.withColumn('Billing2_Start_Date',date_sub(F.col('Billing2_End_Date'),29))
    billingcycle = billingcycle.withColumn('Next_Billing_StartDate',date_add(F.col('Billing1_End_Date'),1))
    billingcycle = billingcycle.withColumn('Next_Billing_EndDate',date_add(F.col('Next_Billing_StartDate'),29))

    billingcycle = billingcycle.select('CONCAT_AGMNT_NO','CUSTOMER_ID','ACCOUNT_SEQ','AGREEMENT_SEQ','Billing1_Start_Date','Billing1_End_Date','Billing2_Start_Date','Billing2_End_Date','Next_Billing_StartDate','Next_Billing_EndDate')
    fileLog("generated bc1, bc2 and bc14 for prediction")

# read usagev1

    usagev1 = spark.sql("select * from "+db_analytical_temp+".EUW_humidity_responsive_customers_Usage_temp")
# take those agreements whose agreement number matches with usagev1

    df1 = billingcycle.alias('df1')
    df2 = usagev1.alias('df2').select('CONCAT_AGMNT_NO').distinct()
    billingcyclev1 = df1.join(df2,['CONCAT_AGMNT_NO']).select('df1.*')
    fileLog("take those agreements whose agreement number matches with usagev1")
    BC_df = billingcyclev1.persist()
    fileLog("counts of unique aggreements after joining are: "+str(BC_df.select('CONCAT_AGMNT_NO').distinct().count()))
# join it with usagewthr_hldy
    UsgWthr_hldy = spark.sql("select * from "+db_analytical_temp+".Euw_customer_aminonami_holiday_usage_temp")
    df1 = UsgWthr_hldy.alias('df1')
    df2 = BC_df.alias('df2')
    ami_nonami_bc = df1.join(df2,["CONCAT_AGMNT_NO","CUSTOMER_ID","ACCOUNT_SEQ","AGREEMENT_SEQ"])
    ami_nonami_bc = ami_nonami_bc.withColumnRenamed("Next_Billing_StartDate","Billing14_Start_Date").withColumnRenamed("Next_Billing_EndDate","Billing14_End_Date")
    fileLog("join it with usagewthr_hldy")
# add the min_diff and max_diff for HDD/CDD variables
    ami_nonami_bc = ami_nonami_bc.withColumn('Min_diff',68-F.col('MIN_DBT'))
    ami_nonami_bc = ami_nonami_bc.withColumn('Max_diff',68-F.col('MAX_DBT'))
    ami_nonami_bc = ami_nonami_bc.persist()
    fileLog("Total counts are : ")
    ami_nonami_bc_count=ami_nonami_bc.count()
    fileLog(ami_nonami_bc_count)
    ami_nonami_bc.createOrReplaceTempView("ami_nonami_bc_temp")
    spark.sql("drop table if exists "+db_analytical_temp+".Euw_customer_aminonami_bc_temp")
    spark.sql("create table "+db_analytical_temp+".Euw_customer_aminonami_bc_temp as select * from ami_nonami_bc_temp")
    column_values=[]
    column_values.insert(0,str(ami_nonami_bc_count))
    column_values.append(str(ami_nonami_bc_count))
    column_values.append('Euw_customer_aminonami_bc_temp')
    print(column_values)
    column_values=','.join(column_values).rstrip(',')
    print(column_values)
    subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,table_name,cur_script,db_analytical_temp,columns,column_values)])

    fileLog("################################## EUW_CUSTOMER_AGREEMENT_BILLING_CYCLE_DETAIL script is complete ###########################################")
