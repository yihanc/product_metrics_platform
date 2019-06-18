# Enter pyspark-cli
# $ pyspark --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2
# 

import re

# Functions to convert Camel Case to Snake Case for column names
def convert_camel_to_snake_case(name):
  s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
  return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

# Generate snake case schema
def gen_snake_schema(schema):
  return StructType(map(lambda x: StructField(convert(x.name), x.dataType, x.nullable), schema))

def main():
  df = {}
  xml_files = [
    ["Posts.xml", "dim"],
    ["Tags.xml", "dim"],
    ["Comments.xml", "dim"],
    ["Users.xml", "dim"],
    ["PostsHistory.xml", "fct"],
  ]

  # Load XML files into data frames
  for xml, dim_type in xml_files:
    df[xml] = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/{}".format(xml))



if __name__ == "__main__":
  main()
    

# df = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Tags.xml")
# df.write.parquet("s3a://stackoverflow-ds/raw/tags.parquet")



# Load Users

# df_users = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Users.xml")
# df_users.write.parquet("s3a://stackoverflow-ds/raw/users.parquet")
