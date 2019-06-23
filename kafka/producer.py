import boto3

s3 = boto3.resource('s3')
obj = s3.Object('stackoverflow-ds', 'PostHistory.xml')
body = obj.get()['Body'].read()
print(body)