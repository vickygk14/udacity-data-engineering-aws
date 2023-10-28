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

# Script generated for node step_trainer_landing
step_trainer_landing_node1696029078108 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project3-glue/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1696029078108",
)

# Script generated for node customer_curated
customer_curated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project3-glue/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696125894364 = ApplyMapping.apply(
    frame=customer_curated_node1,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("timeStamp", "bigint", "timeStamp", "long"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696125894364",
)

# Script generated for node Join
Join_node1696029155287 = Join.apply(
    frame1=step_trainer_landing_node1696029078108,
    frame2=RenamedkeysforJoin_node1696125894364,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1696029155287",
)

# Script generated for node Drop Fields
DropFields_node1696037252238 = DropFields.apply(
    frame=Join_node1696029155287,
    paths=[
        "right_serialNumber",
        "birthDay",
        "timeStamp",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1696037252238",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696126513520 = DynamicFrame.fromDF(
    DropFields_node1696037252238.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696126513520",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696126513520,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-project3-glue/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node2",
)

job.commit()
