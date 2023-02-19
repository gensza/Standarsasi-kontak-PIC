#=========================================# 
#           Import Dependencies           #
#=========================================#
import time, os, pytz
import pandas as pd
import csv
import numpy as np
from datetime import datetime, date
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
#=========================================#
#              Pandas Config              #
#=========================================#
pd.options.display.html.table_schema=True
pd.options.display.max_columns=999
pd.options.display.max_rows=999
#=========================================# 
#          Function get latest DS         #
#=========================================#

################ Function Write Table##################


###############################################################################

def get_list_partition(table):
  partitions = spark.sql("""
    SHOW PARTITIONS {}
  """.format(table)).sort('partition', ascending=False).collect()
  if len(partitions) != 0:
    list_partition = []
    row = partitions [0]
    print(row[0].split('=')[1])
    return row[0].split('=')[1]

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

src_db_ip = os.getenv('zabbix_ip')
src_db_port = os.getenv('zabbix_port')
src_user = os.getenv('zabbix_user')
src_pass = os.getenv('zabbix_password')
src_db_name = os.getenv('zabbix_database')
src_tb_name = "history_uint"
hive_tb_name = "inf.ncp_zabbix_tb_history_uint"

#################### last date tgl auto #######################

local_dt = (datetime.utcnow()+ timedelta(hours=7)).date()
print("Start Ingest = ",local_dt)
last_date = (local_dt - date(1970,1,1)).days * 86400 - (7*3600)

###############################################################

is_empty = True
time_range = 60*60 #1 menit

############### Jika Table nya Sudah ada Run Ini ####################

try:
  last_update = get_list_partition(hive_tb_name)
  last_update = (datetime.strptime(last_update,'%Y%m%d')+ timedelta(days=1)).strftime("%Y%m%d")
  print(last_update)
  df0 = spark.sql("SELECT min(clock) FROM {} where ds='{}'".format(hive_tb_name, last_update))
  df0 = df0.toPandas().iloc[0]
  print(int(df0))
  df0 = datetime.strptime(last_update,'%Y%m%d')
  min_clock = df0.timestamp()-(7*3600)
  max_clock = min_clock + time_range  
  is_empty = False
  print(datetime.fromtimestamp(min_clock + (7*3600)).strftime('%c'), datetime.fromtimestamp(max_clock + (7*3600)).strftime('%c'))
except Exception as e:
  print(e)
  
print(is_empty)
print(last_update)

if not is_empty:
  delete_1_day_before = "TRUNCATE TABLE {} partition (ds = '{}')".format(hive_tb_name, last_update)
  print(delete_1_day_before)
  delete_1_day_before = spark.sql(delete_1_day_before)
  
else:
  latest_day = datetime.strptime(last_update,'%Y%m%d')
  print(latest_day.timestamp())
  min_clock = latest_day.timestamp()-(7*3600)
  print(min_clock)
  max_clock = int(min_clock + time_range)
  print(datetime.fromtimestamp(min_clock + (7*3600)).strftime('%c'), datetime.fromtimestamp(max_clock + (7*3600)).strftime('%c'))

#kode delete_1_daye_before digunakan untuk menghapus data pada partisi terakhir
#data pada partisi terakhir dihapus untuk mengantisipasi anomali data history

####################################################################
  
#last_update merupakan kode untuk menentukan hari terakhir yang tercatat dalam tabel history di hive
#min_clock merupakan pukul 00:00 dari partisi terakhir
#max_clock merupakan batas pengambilan data untuk tiap proses dalam looping


dataitem="./Zabbix NCP/Automation/itemid.csv"    
with open(dataitem, newline='') as e:
    reader = csv.reader(e)
    data = list(reader)
sub_lists = np.array_split(data[0], 2)

    
    
count=0
n= 0
#count digunakan untuk menghitung jumlah data yang telah diingest

#MYSQL
print(datetime.fromtimestamp(max_clock).strftime('%c'))
clock_first = min_clock
print(datetime.fromtimestamp(last_date).strftime('%c'))

while max_clock <= last_date:
  
  start = time.time()
  jampagi = str(datetime.fromtimestamp(clock_first+ (12*3600)).time())
  jamsore = str(datetime.fromtimestamp(clock_first+ (24*3600)).time())
  clock_ingest = str(datetime.fromtimestamp(min_clock+ (7*3600)).time())
  
  name_day = (datetime.fromtimestamp(min_clock+ (7*3600)).date()).strftime("%a")
  fullname_day = (datetime.fromtimestamp(min_clock+ (7*3600)).date()).strftime("%A")
  
  print(str(datetime.fromtimestamp(min_clock+ (7*3600)).date()))
  if (name_day == 'Sat' or name_day == 'Sun') :
    print("it's {} {}, so i want to enjoy my holiday guys".format(clock_ingest,fullname_day))
  else :
    print("it's {} {}, time to working guys".format(clock_ingest,fullname_day))
    print(clock_ingest, '>=' ,jampagi, ' - ', clock_ingest, '<' ,jamsore)
  
    if clock_ingest >= jampagi and clock_ingest < jamsore:
      print("silahkan ingest")
      forcount = 1
      for i in sub_lists:
        print("data list ", forcount)
        itemid = "','".join(list(i))

        print('processing...')
        print('searching between {} - {}'.format(datetime.fromtimestamp(min_clock + (7*3600)).strftime('%c'), datetime.fromtimestamp(max_clock+(7*3600)).strftime('%c')))
        print('last date {}'.format(datetime.fromtimestamp(last_date + (7*3600)).strftime('%c')))
        statement = """(SELECT *,DATE_FORMAT(FROM_UNIXTIME(clock), '%Y-%m-%d %H:00:00') as hours,
                        DATE_FORMAT(FROM_UNIXTIME(clock), '%Y%m%d') as ds
                        FROM {} 
                        where itemid in ('{}')
                        and (clock >= {} and clock < {}) ORDER BY clock DESC) AS final """.format(src_tb_name, itemid, min_clock,max_clock)
        df = spark.read.format("jdbc")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("url","jdbc:mysql://{}:{}/{}".format(src_db_ip, src_db_port, src_db_name))\
            .option("user", src_user )\
            .option("password", src_pass)\
            .option("useSSL", "false")\
            .option("dbtable",statement)\
            .load()

        df = df.groupby(['itemid','hours']).agg(f.max("clock").alias("clock"),
                                                f.max('value').alias('val_max'),
                                               f.mean('value').alias('val_mean'),
                                               f.max("ns").alias("ns"),
                                               f.max("ds").alias("ds"))

        df = df.withColumn("val_max", col("val_max").cast(DecimalType(precision=20, scale=0)))\
               .withColumn("val_mean", col("val_mean").cast(DecimalType(precision=20, scale=0)))
        
        count = df.count()


        print("num rows after :",count )
        #looping dilakukan tiap jam dari waktu awal ingestion hingga tanggal saat ingestion 
        #kode df merupakan kode untuk membuat dataframe dari spark session yang telah dibuat
        #output kode df berupa spark dataframe
        #kode option berfungsi untuk mengisi parameter 
        #dalam penarikan data dari 'data sumber'
        #pada bagian option('dbtable'), query diinput sesuai kebutuhan data
        hiveDB = "inf"
        hiveTable = "ncp_zabbix_tb_history_uint"

        #df.withColumn digunakan untuk menambahkan kolom pada spark.dataframe
        #f.date_format() digunakan untuk mengkonversi data clock pada tabel history
        #menjadi format tanggal YYYY-mm-dd
        #hiveDB merupakan nama schema/database tempat data yang diambil dari source akan disimpan
        #hiveTable merupakan nama table saat disimpan ke dalam hive

        if count == 0:
          print('no data will write to database')

        #kondisi ketika data yang ditarik di rentang waktu tersebut 0
        #maka dimunculkan keterangan bahwa tidak ada data

        else:
          print('append to database ...')
#          df.write.partitionBy('ds').format("parquet").mode("append").saveAsTable("{}.{}".format(hiveDB, hiveTable))
          writeTable(df, hiveDB, hiveTable)

        #sekarang min_clock nilainya adalah nilai max_clock
        #max_clock kemudian ditambahkan 1 jam (3600 detik)
        #count kemudian ditambahkan untuk setiap dataframe

        print('{} total rows'.format(count))

        end = time.time()
        endtime= end - start
        endtime=endtime/60
        print("running time df spark {} menit".format(endtime))
        #ketika data pada waktu tersebut tidak 0
        #maka data di write ke hive
        #tabel dipartisi berdasarkan kolom ds
        #mode yang digunakan adalah append sehingga 
        #data baru akan ditambahkan ke tabel dengan nama sama
        forcount+=1
  min_clock = max_clock
  n+=1
  max_clock += time_range

print('table is up to date, try updating in the next day')

os.system("impala-shell -i n14.bigdata.bri.co.id:25003 -q 'compute stats inf.ncp_zabbix_tb_history_uint;'")
os.system("impala-shell -i n14.bigdata.bri.co.id:25003 -q 'invalidate metadata inf.ncp_zabbix_tb_history_uint;'")
    
t = datetime(1,1,1)+timedelta(seconds=int(time.time()-t0))
deltatime = "{0}:{1}:{2}".format(str(t.hour).zfill(2),str(t.minute).zfill(2),str(t.second).zfill(2))
print("ETL took {}".format(deltatime))

#variabel t digunakan untuk mendefinisikan selang waktu proses
#variabel deltatime digunakan untuk mendefinisikan output selang waktu proses
#yang kemudian dicetak setelah kalimat 'ETL took '

spark.stop()
#spark.stop() digunakan untuk menutup sebuah spark session

