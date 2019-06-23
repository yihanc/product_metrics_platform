import boto3

client = boto3.client('s3') #low-level functional API


obj = client.get_object(Bucket='stackoverflow-ds', Key='PostHistory.xml')

print(obj)
grid_sizes = pd.read_csv(obj['Body'])