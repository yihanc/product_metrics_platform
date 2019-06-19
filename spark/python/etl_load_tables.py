# Enter pyspark in cluster mode
# $pyspark --master spark://10.0.0.5:7077 --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-xml_2.11:0.5.0 —executor-memory 6G
#

from pyspark.sql.types import *
from pyspark.sql.functions import lit
import datetime

### Load Comments.xml

df_comments = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Comments.xml")
today = datetime.datetime.now().strftime("%Y-%m-%d")

# Adding a load_date column for partition
df_comments_done = df_comments.withColumn("load_date", lit(today))

# Load into Parquet File
df_comments_done.write.parquet("s3a://stackoverflow-ds/raw/{}/comments.parquet".format(today))


### Load Posts.xml

from pyspark.sql.types import *

df_posts = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/Posts.xml")

df_comments_done.write.parquet("s3a://stackoverflow-ds/raw/{}/comments.parquet".format(today))
### Load PostHistory.xml



from pyspark.sql.types import *

df_ph = spark.read.format("com.databricks.spark.xml").options(rowTag="row").load("s3a://stackoverflow-ds/PostHistory.xml")
