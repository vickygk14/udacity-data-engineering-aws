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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1696127161716 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project3-glue/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1696127161716",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project3-glue/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1",
)

# Script generated for node customer_trusted
customer_trusted_node1696127098220 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project3-glue/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1696127098220",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696127316174 = ApplyMapping.apply(
    frame=customer_trusted_node1696127098220,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696127316174",
)

# Script generated for node Join
Join_node1696127260403 = Join.apply(
    frame1=step_trainer_trusted_node1696127161716,
    frame2=RenamedkeysforJoin_node1696127316174,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1696127260403",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696127364209 = DynamicFrame.fromDF(
    Join_node1696127260403.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696127364209",
)

# Script generated for node Join
Join_node1696127463169 = Join.apply(
    frame1=DropDuplicates_node1696127364209,
    frame2=accelerometer_trusted_node1,
    keys1=["sensorReadingTime", "right_email"],
    keys2=["timeStamp", "user"],
    transformation_ctx="Join_node1696127463169",
)

# Script generated for node Drop Fields
DropFields_node1696170839841 = DropFields.apply(
    frame=Join_node1696127463169,
    paths=[
        "right_serialNumber",
        "shareWithPublicAsOfDate",
        "right_birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "right_customerName",
        "right_email",
        "lastUpdateDate",
        "right_phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1696170839841",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696170890925 = DynamicFrame.fromDF(
    DropFields_node1696170839841.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696170890925",
)

# Script generated for node Amazon S3
AmazonS3_node1696170899124 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696170890925,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-project3-glue/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1696170899124",
)

job.commit()
