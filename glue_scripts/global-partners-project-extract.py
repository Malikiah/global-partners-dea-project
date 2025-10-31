import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
import pandas as pd
import pyspark.pandas as ps

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sparkContext.setLogLevel("DEBUG")

# JDBC connection properties
jdbc_url = "jdbc:sqlserver://database-1.ck560wqmi0d0.us-east-1.rds.amazonaws.com:1433;databaseName=global_partners"
jdbc_properties = {
    "user": "admin",
    "password": "REDACTED",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Table to Read
schema = "dbo"
table_names = ["date_dim", "order_item_options", "order_items"]

for table_name in table_names:
    full_table = f"{schema}.{table_name}"

    df = spark.read.jdbc(url=jdbc_url, table=full_table, properties=jdbc_properties)

    df.printSchema()

    if table_name == "date_dim":
        df = df.withColumn(
            "is_weekend", col("is_weekend").cast("boolean")
        )

    df.printSchema()
    # Write to S3 in Parquet
    s3_path = f"s3://global-partners-project-bronze/{table_name}/"
    df.write.mode("overwrite").parquet(s3_path)

print(f"Data written to S3 at {s3_path}")

job.commit()