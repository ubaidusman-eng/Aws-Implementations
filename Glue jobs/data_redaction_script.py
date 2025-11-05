import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.sql.functions import concat_ws

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default data quality rule
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# --- Read CSV from S3 ---
AmazonS3_node1760988522788 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://data-redaction/fake_pii_data.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1760988522788"
)

# --- Detect Sensitive Data ---
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    AmazonS3_node1760988522788, ["PERSON_NAME", "EMAIL", "CREDIT_CARD"], 0.25, 0.9, "HIGH"
)

items = classified_map.items()
schema = StructType([
    StructField("columnName", StringType(), True),
    StructField("entityTypes", ArrayType(StringType(), True))
])

# Create DataFrame from detected entities
data_frame = spark.createDataFrame(data=items, schema=schema)

# Flatten array column to string (needed for CSV)
data_frame_flat = data_frame.withColumn("entityTypes", concat_ws(",", "entityTypes"))

# --- Write a single CSV to S3 ---
data_frame_flat.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("s3://data-redaction/output_pii_data/")

job.commit()
