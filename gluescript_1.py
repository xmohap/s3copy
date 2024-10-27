#Python Glue Scripts
#Python Glue Scripts


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)

# Define the source and target S3 paths
source_s3_path = "s3://your-source-bucket/path/to/data.csv"
target_s3_path = "s3://your-target-bucket/path/to/output.csv"

# Read data from S3
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_s3_path]},
    format="csv",
    format_options={"withHeader": True}
)

# Write the data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=datasource0,
    connection_type="s3",
    connection_options={"path": target_s3_path},
    format="csv"
)
