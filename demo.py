from pyspark.sql.functions import *
from pyspark.sql.types import *  
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo").getOrCreate()

data = [(1,"Shiwam"),(2,"Sonu")]
df = spark.createDataFrame(data = data, schema= ["id","Name"])

df.show()

input("Press Enter to exit...")
