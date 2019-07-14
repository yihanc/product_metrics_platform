# Metrics as a Service

This is a project I did during Insight Data Engineering program.

### What did I build?
Providing meaningful metrics and improving metrics’ quality is significant for data engineers. But one of problems is s of our it is hard to manage them across the teams, and it is also hard to find out what metrics are available to use. So, I built a centralized metric service platform which can let user choose metrics,  filter conditions they want and show results and dashboards easily and fast. 

### Web App Link

http://dataprocessing.live

### Youtube Demo 

https://www.youtube.com/watch?v=FUcbyRGpDe8

### Features

- Adopted Lambda Architecture for advantage of both batch and stream-processing

### Architecture Diagram

### Tech Stack

- Dash (v0.43.0) - Web App
- Druid (v0.14.2). Lighting fast query engine with latency (0.5s ~ 5s). Doesn't support joins.
- Presto on EMR (v0.21.4) - Default query engine with latency around (10s ~ 1m).
- Kafka (v2.12-2.2.0) - Speed Layer for live data.
- PySpark (v2.4.3) - Batch Layer for Data Processing.
- AWS S3 - Deep Storage

### Steps to start the Web App

1. Git clone the repo

2. Install Python Packages
```
pip-3.6 install --user dash==0.43.0 
pip-3.6 install --user dash-daq==0.1.0
pip-3.6 install --user presto-python-client
pip-3.6 install --user kafka-python
```
3. Start the app

```
cd product_metrics_platform/app
nohup python3 app.py &
```

4. Start Kafka Producer program for live data

```
cd product_metrics_platform/kafka
nohup ./producer.py &
```

5. Visit URL in your browser
```
http://dataprocessing.live
```

