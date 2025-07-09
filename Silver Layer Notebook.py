from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, to_date, regexp_replace
bronze_df=spark.read.table("workspace.default.customer_transactions_large")
silver_df = bronze_df.withColumn("transaction_Date", regexp_replace(col("transaction_date"), "-", "/"))\
    .withColumn("transaction_Date", to_date(col("transaction_Date"), "MM/dd/yyyy"))\
        .dropna()\
            .dropDuplicates()\
                .withColumn("amount", when(col("amount") < 0, -col("amount")).otherwise(col("amount")))
                
silver_df.write.format("delta").mode("overwrite").saveAsTable("default.customer_transactions_silver")
silver_df.show()