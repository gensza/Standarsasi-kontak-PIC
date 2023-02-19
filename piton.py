#=========================================# 
#           Import Dependencies           #
#=========================================#
import time, os
import csv
from datetime import datetime, date
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

#=========================================# 
#          Membuat Session Spark          #
#=========================================#
spark = SparkSession \
        .builder \
        .appName("Update zabbix_tb_history_uint") \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "16g") \
        .config("spark.network.timeout", 60) \
        .config("spark.yarn.executor.memoryOverhead", "7g")\
        .config("spark.yarn.queue", "root.cdhdesv")\
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")\
        .enableHiveSupport() \
        .getOrCreate()
print("Spark version : " + spark.version)

#=========================================# 
#               Parameter                 #
#=========================================#

t0 = time.time()

src_db_ip = os.getenv('localhost')
src_db_port = os.getenv('3306')
src_user = os.getenv('root')
src_pass = os.getenv('')
src_db_name = os.getenv('wanvolution_dev_backup')
src_tb_name = "tb_kanca"
hive_tb_name = "inf.ncp_zabbix_tb_history_uint"


statement = """(SELECT * FROM {} ) """
df = spark.read.format("jdbc")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("url","jdbc:mysql://{}:{}/{}".format(src_db_ip, src_db_port, src_db_name))\
    .option("user", src_user )\
    .option("password", src_pass)\
    .option("useSSL", "false")\
    .option("dbtable",statement)\
    .load()

df.show(10)