from pyspark.sql import SparkSession

def get_spark(app_name="BostaTask"):
    return SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .getOrCreate()