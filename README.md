# Product Metrics Platform 

This is a project I did during Insight Data Engineering program.

### What did I build?
Providing meaningful metrics and improving metrics’ quality is significant for data engineers. But one of problems is s of our it is hard to manage them across the teams, and it is also hard to find out what metrics are available to use. So, I built a centralized metric service platform which can let user choose metrics,  filter conditions they want and show results and dashboards easily and fast. 

### 

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
python3 app.py
```

4. Visit URL
```
http://dataprocessing.live
```

