import os
import json
import boto3

FOLDER_PATH = "./hh"
S3_ENDPOINT = "http://127.0.0.1:9000"
ACCESS_KEY = "test"
SECRET_KEY = "test"
BUCKET_NAME = "hh-lake"
OUTPUT_FILENAME = "vacancies.jsonl"


def setup_s3():
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except Exception:
        pass
    return s3


def convert_and_upload(s3_client):
    if not os.path.exists(FOLDER_PATH):
        print(f"Папка {FOLDER_PATH} не найдена")
        return

    total_vacancies = 0
    temp_file = "temp_vacancies.jsonl"

    with open(temp_file, 'w', encoding='utf-8') as out:
        for filename in os.listdir(FOLDER_PATH):
            if not filename.endswith('.json'):
                continue

            filepath = os.path.join(FOLDER_PATH, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        for item in data:
                            out.write(json.dumps(item, ensure_ascii=False) + '\n')
                            total_vacancies += 1
                    else:
                        out.write(json.dumps(data, ensure_ascii=False) + '\n')
                        total_vacancies += 1
                except json.JSONDecodeError:
                    pass

    print(f"Собрано вакансий: {total_vacancies}")
    s3_client.upload_file(temp_file, BUCKET_NAME, OUTPUT_FILENAME)
    os.remove(temp_file)
    print(f"Загружено в s3://{BUCKET_NAME}/{OUTPUT_FILENAME}")


if __name__ == "__main__":
    s3 = setup_s3()
    convert_and_upload(s3)
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    for obj in response.get('Contents', []):
        print(f"Файл: {obj['Key']}, Размер: {obj['Size'] / (1024 * 1024):.2f} MB")