# Databricks notebook source
# MAGIC %pip install /dbfs/user/yurii-ncube@xendit.co/spark-datastream-test/libraries/xsh_streaming_applications/xsh_streaming_applications-0.0.11-py3-none-any.whl
# MAGIC %pip install /dbfs/user/yurii-ncube@xendit.co/spark-datastream-test/libraries/xsh_python_event_models/xsh_python_event_models-0.70.0-py3-none-any.whl

# COMMAND ----------
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# COMMAND ----------
print("hey")

# COMMAND ----------
columns = ["bid","amount", "product_type", "timestamp"]
data = [
    ("627b42529e71a9351111111", "400", "virtual_account", "2020-04-04T04:04:04"),
    ("627b42529e71a9351111111", "300", "virtual_account", "2020-04-04T04:44:04"),
    ("627b42529e71a9351111142", "3000", "virtual_account", "2020-04-04T04:24:04"),
    ("627b42529e71a9351111111", "400", "virtual_account", "2020-04-04T04:14:04"),
    ("627b42529e71a9351111102", "3000", "virtual_account", "2020-04-04T04:34:04"),
    ("627b42529e71a9351111111", "200", "virtual_account", "2020-04-04T04:24:04"),
    ("627b42529e71a9351111121", "3000", "virtual_account", "2020-04-04T04:14:04"),
    ("627b42529e71a9351111111", "1000", "virtual_account", "2020-04-04T04:44:04"),
    ("627b42529e71a9351111128", "3000", "virtual_account", "2020-04-04T04:54:04"),
    ("627b42529e71a9351111111", "70000", "virtual_account", "2020-04-04T04:44:04"),
    ("627b42529e71a9351111122", "3000", "virtual_account", "2020-04-04T04:63:04"),
    ("627b42529e71a9351111111", "400", "virtual_account", "2020-04-04T04:53:04"),
    ("627b42529e71a9351111124", "3000", "virtual_account", "2020-04-04T04:43:04"),
    ("627b42529e71a9351111111", "100", "virtual_account", "2020-04-04T04:23:04"),
    ("627b42529e71a9351111123", "3000", "virtual_account", "2020-04-04T04:13:04"),
]

# COMMAND ----------
df = spark.createDataFrame(data)
type(df)
