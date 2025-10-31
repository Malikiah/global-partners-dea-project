import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, TimestampType, DateType, ShortType, ByteType,
    DecimalType
)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'TempDir',
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = 'dbo'
table_names = ["date_dim", "order_item_options", "order_items"]

for table_name in table_names:
    s3 = "s3://global-partners-project-bronze"
    s3_path = [f"{s3}/{table_name}/"]

    print(f"Pulling data from {s3_path}")
    source_dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": s3_path
        },
        format="parquet",
        transformation_ctx=f"source_{table_name}"
    )

    spark_df = source_dynamic_frame.toDF()

    if table_name == "date_dim" and "is_weekend" in spark_df.columns:
        print("Casting 'is_weekend' column to boolean...")
        spark_df = spark_df.withColumn("is_weekend", col("is_weekend").cast("boolean"))

    # Convert back to a DynamicFrame for the Redshift writer
    frame_to_write = DynamicFrame.fromDF(
        spark_df,
        glueContext,
        f"transformed_frame_{table_name}"
    )

    print("Writing data to Redshift...")
    glueContext.write_dynamic_frame.from_options(
        frame=frame_to_write,
        connection_type="jdbc",
        connection_options={
            "url": "jdbc:redshift://default-workgroup.918346807626.us-east-1.redshift-serverless.amazonaws.com:5439/dev",
            "database": "dev",
            "dbtable": f"public.{table_name}",
            "user": "admin",
            "password": "REDACTED",
        },
        transformation_ctx=f"write_{table_name}"
    )

job.commit()