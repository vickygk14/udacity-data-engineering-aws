import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-project3-glue",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1695952613546 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-project3-glue",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1695952613546",
)

# Script generated for node Join
Join_node1695952696898 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometer_landing_node1695952613546,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1695952696898",
)

# Script generated for node Drop Fields
DropFields_node1695953030409 = DropFields.apply(
    frame=Join_node1695952696898,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1695953030409",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696041820697 = DynamicFrame.fromDF(
    DropFields_node1695953030409.toDF().dropDuplicates(["email", "serialnumber"]),
    glueContext,
    "DropDuplicates_node1696041820697",
)

# Script generated for node Amazon S3
AmazonS3_node1696123903437 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696041820697,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-project3-glue/customer_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1696123903437",
)

job.commit()
