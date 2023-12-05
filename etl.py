from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StringType, StructField, StructType
from datetime import datetime,timedelta
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CSV to Parquet Conversion") \
    .getOrCreate()

current_date = (datetime.utcnow() - timedelta(days=1)).date()
# Define the schema for the CSV data
schema = StructType([
    StructField("jobId", StringType(), True),
    StructField("employerId", StringType(), True),
    StructField("employerName", StringType(), True),
    StructField("employerProfileId", StringType(), True),
    StructField("employerProfileName", StringType(), True),
    StructField("jobTitle", StringType(), True),
    StructField("locationName", StringType(), True),
    StructField("minimumSalary", StringType(), True),
    StructField("maximumSalary", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("expirationDate", StringType(), True),
    StructField("date", StringType(), True),
    StructField("jobDescription", StringType(), True),
    StructField("applications", StringType(), True),
    StructField("jobUrl", StringType(), True)
])

# S3 path to the CSV file
s3_input_path = f"s3://data-programming-bdat1004-web-app/temp/jobs_export/dt={str(current_date)}/"
s3_output_path = f"s3://data-programming-bdat1004-web-app/temp/processed_jobs_export/dt={str(current_date)}/"


# Read CSV file into Spark DataFrame with defined schema
df = spark.read.csv(s3_input_path, header=True, schema=schema)

# Convert columns 'date' and 'expirationDate' to DateType with specified date format
date_format = "dd/MM/yyyy"  # Adjust this format based on your date string format
df = df.withColumn("date", to_date("date", date_format))
df = df.withColumn("expirationDate", to_date("expirationDate", date_format))



# Print the schema of the DataFrame
df.printSchema()
df.write.mode("overwrite").parquet(s3_output_path, compression="gzip")
