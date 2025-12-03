import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, minute, dayofweek, weekofyear, month, year, concat_ws, to_timestamp, to_date, split, trim, regexp_replace, round, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import functions as F

# -----------------------------
# 1️⃣ Spark session and config
# -----------------------------
postgres_jar = os.getenv("POSTGRES_JAR", "/app/jars/postgresql-42.6.0.jar")
spark = (
    SparkSession.builder.appName("AustinTripsETL")
    .config("spark.jars", postgres_jar)
    .getOrCreate()
)

# -----------------------------
# 2️⃣ Load Trips & Kiosk CSVs
# -----------------------------
trips_path = "/app/data/raw/trips.csv"
kiosk_path = "/app/data/raw/Kiosks.csv"

df = spark.read.option("header", True).csv(trips_path)
kiosk_df = spark.read.option("header", True).csv(kiosk_path)

# Cast Trip Duration to integer
df = df.withColumn("TripDurationMinutes", col("Trip Duration Minutes").cast(IntegerType()))
df.show(5, truncate=False)
# -----------------------------
# 3️⃣ Create Time Dimension
# -----------------------------
# ✅ Derive hour, minute, 15-min bucket
df = df.withColumn("Checkout Datetime", F.to_timestamp(col("Checkout Datetime"), "MM/dd/yyyy hh:mm:ss a"))

# Derive date, hour, minute, and 15-min bucket
df = df.withColumn("Checkout Date", F.to_date(col("Checkout Datetime")))
df = df.withColumn("Hour", F.hour(col("Checkout Datetime")))
df = df.withColumn("Minute", F.minute(col("Checkout Datetime")))
df = df.withColumn("Time15Min", F.floor((col("Hour") * 60 + col("Minute")) / 15))

# ✅ Build dim_time with readable 15-min intervals
dim_time = (
    df.select("Time15Min", "Hour", "Minute")
      .distinct()
      .withColumnRenamed("Time15Min", "TID")
      .withColumnRenamed("Hour", "HourOfDay")
      .withColumn(
          "MinuteOfBucket",  # start minute of each 15-min bucket
          (F.col("TID") % 4) * 15
      )
      .withColumn(
          "TimeLabel",  # e.g. "08:15", "14:30"
          F.concat_ws(
              ":",
              F.lpad(F.col("HourOfDay").cast("string"), 2, "0"),
              F.lpad(F.col("MinuteOfBucket").cast("string"), 2, "0")
          )
      )
      .orderBy("TID")
)
# -----------------------------
# 4️⃣ Create Date Dimension
# -----------------------------
df = df.withColumn("DayOfWeek", date_format(col("Checkout Date"), "EEEE"))
df = df.withColumn("MonthOfYear", date_format(col("Checkout Date"), "MMMM"))
df = df.withColumn("Year", year(col("Checkout Date")))

# Add numerical columns for sorting
df = df.withColumn("DayOfWeekNum", F.dayofweek(col("Checkout Date")))  # 1=Sunday, 7=Saturday
df = df.withColumn("MonthOfYearNum", F.month(col("Checkout Date")))    # 1=January, 12=December

# Build date dimension with both names and numeric values
dim_date = (
    df.select("DayOfWeek", "DayOfWeekNum", "MonthOfYear", "MonthOfYearNum", "Year")
      .distinct()
      .withColumn("DID", F.row_number().over(
          Window.orderBy("Year", "MonthOfYearNum", "DayOfWeekNum")
      ))
)


# -----------------------------
# 5️⃣ Load Location Dimension directly (no join)
# -----------------------------
dim_location = kiosk_df.select(
    col("Kiosk ID").alias("LID"),
    col("Kiosk Name").alias("KioskName"),
    round(trim(split(regexp_replace(col("Location"), "[\(\)\n]", ""), ",").getItem(0)).cast(DoubleType()), 4).alias("Latitude"),
    round(trim(split(regexp_replace(col("Location"), "[\(\)\n]", ""), ",").getItem(1)).cast(DoubleType()), 4).alias("Longitude")
).distinct()

# -----------------------------
# 6️⃣ Create Junk Dimension
# -----------------------------
dim_junk = df.select(
    col("Membership or Pass Type").alias("Subscription"),
    col("Bike Type").alias("BikeType")
).distinct() \
    .withColumn("JID", concat_ws("_", col("Subscription"), col("BikeType")))

# -----------------------------
# 7️⃣ Aggregate Fact Table
# -----------------------------
fact_df = df.join(dim_time, df["Time15Min"] == dim_time["TID"]) \
    .join(dim_date, (df["MonthOfYear"] == dim_date["MonthOfYear"]) & (df["Year"] == dim_date["Year"])& (df["DayOfWeek"]==dim_date["DayOfWeek"])) \
    .join(dim_junk, (df["Membership or Pass Type"] == dim_junk["Subscription"]) &
                    (df["Bike Type"] == dim_junk["BikeType"]))

fact_df = fact_df.groupBy("TID", "DID", "Checkout Kiosk ID", "JID") \
    .agg({"TripDurationMinutes": "sum", "*": "count"}) \
    .withColumnRenamed("sum(TripDurationMinutes)", "TripDuration") \
    .withColumnRenamed("count(1)", "TripCount") \
    .withColumnRenamed("Checkout Kiosk ID", "LID")

# -----------------------------
# 8️⃣ Write to Postgres
# -----------------------------
db_user = os.getenv("POSTGRES_USER", "admin")
db_password = os.getenv("POSTGRES_PASSWORD", "secret")
db_name = os.getenv("POSTGRES_DB", "mydb")
db_host = os.getenv("POSTGRES_HOST", "postgres")
db_port = os.getenv("POSTGRES_PORT", "5432")

props = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver",
}
jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

dim_time.write.jdbc(url=jdbc_url, table="dim_time", mode="overwrite", properties=props)
dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=props)
dim_location.write.jdbc(url=jdbc_url, table="dim_location", mode="overwrite", properties=props)
dim_junk.write.jdbc(url=jdbc_url, table="dim_junk", mode="overwrite", properties=props)
fact_df.write.jdbc(url=jdbc_url, table="fact_trips", mode="overwrite", properties=props)

print("✅ Sample rows after parsing:")
df.select("Checkout Datetime", "Checkout Date", "Hour", "Minute", "DayOfWeek").show(5, truncate=False)

spark.stop()
print("ETL completed successfully without any errors!!")
