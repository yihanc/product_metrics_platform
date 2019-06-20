export PATH=/usr/local/spark/bin:$PATH
  
alias spa="pyspark --master spark://10.0.0.5:7077 --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-xml_2.11:0.5.0 --executor-memory 4G"
alias pres="presto-cli --catalog hive --schema web"
alias sw="cd ~/insight_di/web/; python3 app.py"
