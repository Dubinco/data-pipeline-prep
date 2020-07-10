from pyspark.sql.functions import lit, when, col
import pyspark.sql.functions as f
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

df_1980 = spark.read.option("header", "true").option("delimiter", ",").csv(
    "movie_ratings_1980_2000_p10.csv")
df_2001 = spark.read.option("header", "true").option("delimiter", ",").csv(
    "movie_ratings_2010_2001_p25.csv")
df_2011 = spark.read.option("header", "true").option("delimiter", ",").csv(
    "movie_ratings_2015_2011_p12.csv")
#ATTENTION le delimiter est différent ici
df_2016 = spark.read.option("header", "true").option("delimiter", ";").csv(
    "movie_ratings_2020_2016_p25.csv")

concat = df_2016.union(df_2011)
concat = concat.union(df_2001)
concat = concat.union(df_1980)
#print(concat.count())
#concat.dropDuplicates()
#concat.show()

concat.write.parquet("result","overwrite")
res = spark.read.parquet("result")
res.show()
