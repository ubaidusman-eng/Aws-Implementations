import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import upper, trim, col, to_date
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_bucket = "s3://redshift-analytics-raw11"

def clean_column_names(df):
    new_cols = [c.strip().replace('.', '_').replace(' ', '_').replace('\ufeff', '') for c in df.columns]
    return df.toDF(*new_cols)

customers = spark.read.option("header", "true").csv(f"{s3_bucket}/raw/customers.csv")
products  = spark.read.option("header", "true").csv(f"{s3_bucket}/raw/products.csv")
sales     = spark.read.option("header", "true").csv(f"{s3_bucket}/raw/sales.csv")

customers = clean_column_names(customers)
products  = clean_column_names(products)
sales     = clean_column_names(sales)

customers = (
    customers
    .withColumn("customer_id", col("customer_id").cast(IntegerType()))
    .withColumn("name", trim(upper(col("name").cast(StringType()))))
)

products = (
    products
    .withColumn("product_id", col("product_id").cast(IntegerType()))
    .withColumn("product_name", trim(upper(col("product_name").cast(StringType()))))
)

sales = (
    sales
    .withColumn("sale_id", col("sale_id").cast(IntegerType()))
    .withColumn("customer_id", col("customer_id").cast(IntegerType()))
    .withColumn("product_id", col("product_id").cast(IntegerType()))
    .withColumn("store", trim(col("store").cast(StringType())))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
)

customers.write.mode("overwrite").parquet(f"{s3_bucket}/processed/customers/")
products.write.mode("overwrite").parquet(f"{s3_bucket}/processed/products/")
sales.write.mode("overwrite").parquet(f"{s3_bucket}/processed/sales/")

job.commit()
