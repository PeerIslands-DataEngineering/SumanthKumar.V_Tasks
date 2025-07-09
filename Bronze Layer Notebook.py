from pyspark.sql import *
bronze_df=spark.read.table("workspace.default.customer_transactions_large")



