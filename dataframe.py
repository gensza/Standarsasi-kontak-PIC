from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Reading from a database").getOrCreate()

# Read data from a MySQL database into a DataFrame
df = spark.read \
  .format("jdbc") \
  .option("url", "jdbc:mysql://localhost:3306/wanvolution_dev_backup") \
  .option("driver", "com.mysql.jdbc.Driver") \
  .option("dbtable", "tb_kanca") \
  .option("user", "root") \
  .option("password", "") \
  .load()

# Show the DataFrame
df.show()