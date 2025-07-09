from pyspark.sql import *
from pyspark.sql.functions import*
silver_df=spark.read.table("default.customer_transactions_silver")
gold_df=silver_df.groupBy("customer_id").agg(count("*").alias("count"),sum("amount").alias("total"),avg("amount").alias("avg_amount"))
gold_df.write.format("delta").mode("overwrite").saveAsTable("default.customer_transactions_gold")