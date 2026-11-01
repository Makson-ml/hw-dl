import boto3

s3 = boto3.client(
    's3',
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

s3.create_bucket(Bucket="hh-lake")
print("Бакет 'hh-lake' успешно создан!")

buckets = s3.list_buckets()
print("Список бакетов:", [b['Name'] for b in buckets['Buckets']])