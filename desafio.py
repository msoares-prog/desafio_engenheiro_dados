from pyspark.sql import SparkSession
import pyspark.sql.functions as F     


# Spark configuration
spark = SparkSession \
    .builder \
    .appName("Desafio engenheiro de dados") \
    .config("spark.debug.maxToStringFields", "number") \
    .getOrCreate()

# Reads the json file
df = spark.read.json("path-to-json-file/data.json")

# Sums of the 'PageViews'
total_pageviews = df.groupBy().agg(F.sum('totals.pageviews'))

#Creates a copy of the dataset
df.createOrReplaceTempView("data")

# Number of sessions per user
sessions_user = spark.sql("SELECT fullVisitorId, visitNumber FROM data")

# Different sessions by date
sessions_date = spark.sql("SELECT date, COUNT(visitNumber) FROM data GROUP BY date")

# Average session duration by date
sessions_timeonsite = spark.sql("SELECT date, MEAN(totals.timeOnSite) FROM data GROUP BY date")

# Daily sessions by browser type
browser = spark.sql("SELECT device.browser, date, COUNT(visitNumber) FROM data GROUP BY date, device.browser")