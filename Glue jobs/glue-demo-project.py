import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col

    # Get the first DynamicFrame from the collection
    dyf = dfc.select(list(dfc.keys())[0])

    # Convert to Spark DataFrame
    df = dyf.toDF()

    # Add a new calculated column
    df = df.withColumn("total_value", col("price") * col("quantity"))

    # Convert back to DynamicFrame
    result_dyf = DynamicFrame.fromDF(df, glueContext, "result_dyf")

    # ✅ Wrap in DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransformOutput": result_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1761679861492 = glueContext.create_dynamic_frame.from_catalog(database="ecommerce_db", table_name="ecommerceraw", transformation_ctx="AWSGlueDataCatalog_node1761679861492")

# Script generated for node Drop Duplicates
DropDuplicates_node1761682581863 =  DynamicFrame.fromDF(AWSGlueDataCatalog_node1761679861492.toDF().dropDuplicates(["order_id"]), glueContext, "DropDuplicates_node1761682581863")

# Script generated for node Change Schema
ChangeSchema_node1761682684413 = ApplyMapping.apply(frame=DropDuplicates_node1761682581863, mappings=[("order_id", "long", "order_id", "long"), ("customer_id", "long", "customer_id", "long"), ("product", "string", "product", "string"), ("category", "string", "category", "string"), ("region", "string", "region", "string"), ("price", "long", "price", "long"), ("quantity", "long", "quantity", "long"), ("order_date", "string", "order_date", "date")], transformation_ctx="ChangeSchema_node1761682684413")

# Script generated for node Custom Transform
CustomTransform_node1761682814274 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1761682684413": ChangeSchema_node1761682684413}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1761746232926 = SelectFromCollection.apply(dfc=CustomTransform_node1761682814274, key=list(CustomTransform_node1761682814274.keys())[0], transformation_ctx="SelectFromCollection_node1761746232926")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1761746232926, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761746202615", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SelectFromCollection_node1761746232926.count() >= 1):
   SelectFromCollection_node1761746232926 = SelectFromCollection_node1761746232926.coalesce(1)
AmazonS3_node1761746241005 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1761746232926, connection_type="s3", format="csv", connection_options={"path": "s3://ecommerce-data111/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1761746241005")

job.commit()