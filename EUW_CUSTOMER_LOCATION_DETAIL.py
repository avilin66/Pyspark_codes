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
from pyspark.sql.functions import row_number
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
    table_name= sys.argv[10]
    cur_script= sys.argv[11]
    source_log_euw=sys.argv[12]
    euw_shell_script_path=sys.argv[13]
    current_dt = str(datetime.now().strftime("%Y%m%d"))
    log_dir = source_log_euw+"/"+current_dt+"/python_script_customer_location_logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    spark = SparkSession.builder.appName("EUW_CUSTOMER_LOCATION_DETAIL").enableHiveSupport().getOrCreate()
    columns="inserted_count,total_count,table_name"
    def fileLog(logMsg):
        with open(log_dir+"/EUW_CUSTOMER_LOCATION_DETAIL_LOG.log", "a") as myfile:
            myfile.write(str(logMsg)+ "\n")
    fileLog("################################## EUW_CUSTOMER_LOCATION_DETAIL script is started ###########################################")
    agr_zip = spark.sql("select C.site_id as site_id,C.us_location_address_id as us_location_address_id,C.customer_id as customer_id,C.account_seq as account_seq,C.agreement_seq as agreement_seq, D.state_cd as state_cd,D.town_code as town_code from (select distinct A.site_id, A.us_location_address_id, B.customer_id,B.account_seq,B.agreement_seq from certified.customer_erp_address_css_addr_usg A INNER JOIN certified.daily_usage B ON A.site_id = B.site_id where B.register_type_cd='KWH') C INNER JOIN certified.CUSTOMER_ERP_ADDRESS_CSS_US_ADDR D ON C.us_location_address_id = D.id where state_cd IN ('CA','OR')")
    agr_zip = agr_zip.withColumn('CONCAT_AGMNT_NO',F.concat(F.col('customer_id'),F.col('account_seq'),F.col('agreement_seq')).cast(LongType()))
    agr_zip = agr_zip.distinct()
# store in hive
    agr_zipv1fl_count=agr_zip.count()
    fileLog("final counts in customer_zip :")
    fileLog(agr_zipv1fl_count)
    agr_zip.createOrReplaceTempView("agr_zip_temp")
    spark.sql("drop table if exists "+db_analytical_temp+".Euw_aggregated_cust_zip_temp")
    spark.sql("create table "+db_analytical_temp+".Euw_aggregated_cust_zip_temp as select * from agr_zip_temp")
    column_values=[]
    column_values.insert(0,str(agr_zipv1fl_count))
    column_values.append(str(agr_zipv1fl_count))
    column_values.append('Euw_aggregated_cust_zip_temp')
    print(column_values)
    column_values=','.join(column_values).rstrip(',')
    print(column_values)
    subprocess.Popen(['bash','-c','. {}/process_control.sh; updateProcessControl %s %s %s %s %s %s %s %s %s %s %s'.format(euw_shell_script_path) %(username,password,db,connection_str,source,group_id,table_name,cur_script,db_analytical_temp,columns,column_values)])
    fileLog("################################## EUW_CUSTOMER_LOCATION_DETAIL script is complete ###########################################")
