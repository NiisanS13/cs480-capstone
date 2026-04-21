import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Raw
Raw_node1774929814862 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://telecom-churn-groupproject/raw/telecom_churn.csv"], "recurse": True}, transformation_ctx="Raw_node1774929814862")

# Script generated for node Transform to Clean
SqlQuery1770 = '''
SELECT 
  customer_id, 
  regexp_replace(telecom_partner, '"', '') AS telecom_partner_cleaned, 
  gender, 
  age, 
  state, 
  city, 
  pincode, 
  date_of_registration, 
  num_dependents, 
  estimated_salary, 
  calls_made, 
  sms_sent, 
  data_used, 
  churn 
FROM myDataSource
'''
TransformtoClean_node1774930120505 = sparkSqlQuery(glueContext, query = SqlQuery1770, mapping = {"myDataSource":Raw_node1774929814862}, transformation_ctx = "TransformtoClean_node1774930120505")

# Script generated for node Clean Bucket
EvaluateDataQuality().process_rows(frame=TransformtoClean_node1774930120505, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1774929734328", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CleanBucket_node1774930465147 = glueContext.write_dynamic_frame.from_options(frame=TransformtoClean_node1774930120505, connection_type="s3", format="glueparquet", connection_options={"path": "s3://telecom-churn-groupproject/cleaned/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="CleanBucket_node1774930465147")

job.commit()


