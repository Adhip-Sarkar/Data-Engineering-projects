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

# Script generated for node daily_raw_flight_data_from_s3
daily_raw_flight_data_from_s3_node1707805599406 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="airlines",
        table_name="daily_raw",
        transformation_ctx="daily_raw_flight_data_from_s3_node1707805599406",
    )
)

# Script generated for node dim_Airport_code_read
dim_Airport_code_read_node1707805652591 = glueContext.create_dynamic_frame.from_catalog(
    database="airlines",
    table_name="dev_airlines_airports_dim",
    redshift_tmp_dir="s3://temp-as-0902",
    transformation_ctx="dim_Airport_code_read_node1707805652591",
)

# Script generated for node Join
Join_node1707805714824 = Join.apply(
    frame1=dim_Airport_code_read_node1707805652591,
    frame2=daily_raw_flight_data_from_s3_node1707805599406,
    keys1=["airport_id"],
    keys2=["originairportid"],
    transformation_ctx="Join_node1707805714824",
)

# Script generated for node dept_airport_schema_changes
dept_airport_schema_changes_node1707806154640 = ApplyMapping.apply(
    frame=Join_node1707805714824,
    mappings=[
        ("city", "string", "dep_city", "string"),
        ("name", "string", "dep_airport", "string"),
        ("state", "string", "dep_state", "string"),
        ("carrier", "string", "carrier", "string"),
        ("destairportid", "long", "destairportid", "long"),
        ("depdelay", "long", "dep_delay", "bigint"),
        ("arrdelay", "long", "arr_delay", "bigint"),
    ],
    transformation_ctx="dept_airport_schema_changes_node1707806154640",
)

# Script generated for node Join
Join_node1707806970758 = Join.apply(
    frame1=dept_airport_schema_changes_node1707806154640,
    frame2=dim_Airport_code_read_node1707805652591,
    keys1=["destairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1707806970758",
)

# Script generated for node Change Schema
ChangeSchema_node1707807010876 = ApplyMapping.apply(
    frame=Join_node1707806970758,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("dep_state", "string", "dep_state", "string"),
        ("state", "string", "arr_state", "string"),
        ("arr_delay", "bigint", "arr_delay", "long"),
        ("city", "string", "arr_city", "string"),
        ("name", "string", "arr_airport", "string"),
        ("dep_city", "string", "dep_city", "string"),
        ("dep_delay", "bigint", "dep_delay", "long"),
        ("dep_airport", "string", "dep_airport", "string"),
    ],
    transformation_ctx="ChangeSchema_node1707807010876",
)

# Script generated for node redshift_fact_table_write
redshift_fact_table_write_node1707807066231 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=ChangeSchema_node1707807010876,
        database="airlines",
        table_name="dev_airlines_daily_flights_fact",
        redshift_tmp_dir="s3://temp-as-0902",
        additional_options={
            "aws_iam_role": "arn:aws:iam::211125617477:role/redshift_role_new"
        },
        transformation_ctx="redshift_fact_table_write_node1707807066231",
    )
)

job.commit()
