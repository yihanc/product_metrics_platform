# Start pyspark in cluster mode

### pyspark --master spark://10.0.0.5:7077 --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-xml_2.11:0.5.0 —executor-memory 6G

# Load Comments.xml

from pyspark.sql.types import *

df_comments = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Comments.xml")


# Load Posts.xml

from pyspark.sql.types import *

df_posts = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Posts.xml")


# Load PostHistory.xml



from pyspark.sql.types import *

df_ph = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/PostHistory.xml")
