# Enter pyspark-cli
# $ pyspark --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2
# 
from pyspark.sql.functions import substring
import re
import jinja2

# Functions to convert Camel Case to Snake Case for column names
def cnvt_camel_to_snake(name):
  s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
  return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

# Generate snake case schema
def gen_transformed_cols(columns):
  return map(lambda x: convert_camel_to_snake_case(x)[1:], columns)


def main():
  df = {}
  
  config = {
    "Posts.xml": {
      "table_type": "dim",
      "partitioned_by": ["_PostTypeId"]
    }
    "Tags.xml": {
      "table_type": "dim",
    }
    "Comments.xml": {
      "table_type": "dim",
    }
    "Users.xml": {
      "table_type": "dim",
    }
    "PostsHistory.xml": {
      "table_type": "fct",
      "partitioned_by": ["_CreationDate", "_PostHistoryTypeId"]
    }
  }
  
  # Load XML files into data frames
  for file, payload in config.items():
    table_type = payload["table_type"]
    partitioned_by = payload["partitioned_by"] if partitioned_by in payload else []
    
    # Read S3 XML into DataFrame
    df[file] = spark.read.format("com.databricks.spark.xml").options(rowTag="row", samplingRatio=0.01).load("s3a://stackoverflow-ds/{}".format(file))
    
    # Create Temp View for Spark SQL
    table = file.split(".")[0].lower()
    df.createOrReplaceTempView(table)
    
    # Read table from select
    cols = df.columns()
    
    # Remove partitioned columns. For Hive Partition Create Syntax
    for col in partitoned_by:
      cols.remove(col)
    
    transformed_cols = gen_transformed_cols(cols)
    
    sql = Template("""
      SELECT
        {% for old, new in zip(cols, transformed_cols) %}
        {{ old }} AS {{ new }},
        {% endfor %}
        
      FROM {{ table }}
    """).render(
      table=table,
      
    )
    
    # SQL Transformation
    transformed_df = spark.sql(sql)
    
    transformed_df.write.parquet("s3a://stackoverflow-ds/raw/{}.parquet".format(table))
    
    # Finished
    print("COMPLETE: Table {} has been saved to s3.".format(table))
      
      
if __name__ == "__main__":
  main()
    

# df = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Tags.xml")
# df.write.parquet("s3a://stackoverflow-ds/raw/tags.parquet")



# Load Users

# df_users = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Users.xml")
# df_users.write.parquet("s3a://stackoverflow-ds/raw/users.parquet")
