import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer-trusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project3-glue/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1",
)

# Script generated for node accelerometer-landing
accelerometerlanding_node1695950821644 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project3-glue/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1695950821644",
)

# Script generated for node Join
Join_node1695951014620 = Join.apply(
    frame1=accelerometerlanding_node1695950821644,
    frame2=customertrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1695951014620",
)

# Script generated for node Drop Fields
DropFields_node1695951447350 = DropFields.apply(
    frame=Join_node1695951014620,
    paths=[
        "email",
        "phone",
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1695951447350",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695951447350,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-project3-glue/accelerometer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node2",
)

job.commit()
