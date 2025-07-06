from pyspark.sql.functions import *
from pyspark.sql.types import *  
from pyspark.sql.window import Window

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo").getOrCreate()

#creating dataframe
data = [("2025-03-01" ,100),("2025-03-02" ,200),("2025-03-03" ,300),("2025-03-04" ,400),
        ("2025-03-05" ,500),("2025-03-06" ,600),("2025-03-07" ,700)]

df = spark.createDataFrame(data = data, schema= ["date","sales_amt"])

#Correcting data type
df = df.select(df.date.cast("date"), df.sales_amt.cast("int"))

df.show()
df.printSchema()

window_spec = Window.orderBy(df.date.asc())

final_df = df.withColumn("moving_avg",avg(df.sales_amt).over(window_spec))
final_df.show()

input("Press Enter to exit...")
