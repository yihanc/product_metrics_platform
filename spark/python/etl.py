# Enter pyspark-cli
# $ pyspark --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2
# 

# Load Tags

df = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Tags.xml")
df.write.parquet("s3a://stackoverflow-ds/raw/tags.parquet")


# Load Users

df_users = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Users.xml")
df_users.write.parquet("s3a://stackoverflow-ds/raw/users.parquet")
