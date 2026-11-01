import boto3
import json
import collections
import itertools
import re

S3_ENDPOINT = "http://127.0.0.1:9000"
ACCESS_KEY = "test"
SECRET_KEY = "test"
BUCKET = "hh-lake"
KEY = "vacancies.jsonl"
CHUNK_SIZE = 2 * 1024 * 1024


def get_s3_client():
    return boto3.client(
        's3', endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
    )


def get_file_size():
    s3 = get_s3_client()
    return s3.head_object(Bucket=BUCKET, Key=KEY)['ContentLength']


def mapper(chunk_data):
    local_counts = collections.defaultdict(int)
    for line in chunk_data.split('\n'):
        line = line.strip()
        if not line:
            continue
        try:
            line = re.sub(r',\s*}', '}', line)
            line = re.sub(r',\s*]', ']', line)

            vac = json.loads(line)
            name_lower = vac.get('name', '').lower()
            skills_raw = vac.get('key_skills', [])

            if isinstance(skills_raw, list):
                skills_list = []
                for s in skills_raw:
                    if isinstance(s, str):
                        skills_list.append(s)
                    elif s is not None:
                        skills_list.append(str(s))
            elif isinstance(skills_raw, str):
                skills_list = [skills_raw]
            elif skills_raw is None:
                skills_list = []
            else:
                skills_list = [str(skills_raw)]

            has_go = 'golang' in name_lower or 'goland' in name_lower

            if not has_go:
                for s in skills_list:
                    s_clean = s.lower().strip()
                    if s_clean == 'go' or s_clean == 'golang' or s_clean == 'goland':
                        has_go = True
                        break

            if not has_go:
                continue

            skills = sorted(list(set([s.lower().strip() for s in skills_list if str(s).strip() != ''])))
            if not skills:
                continue

            for s in skills:
                local_counts[('S', s)] += 1
            for pair in itertools.combinations(skills, 2):
                local_counts[('P', pair)] += 1
            for triple in itertools.combinations(skills, 3):
                local_counts[('T', triple)] += 1
        except json.JSONDecodeError:
            continue
    return local_counts


def reducer(mapped_results):
    final_counts = collections.defaultdict(int)
    for chunk_counts in mapped_results:
        for k, v in chunk_counts.items():
            final_counts[k] += v
    return final_counts


if __name__ == "__main__":
    s3 = get_s3_client()
    file_size = get_file_size()
    chunks_data = []
    start = 0
    chunk_num = 0

    while start < file_size:
        length = min(CHUNK_SIZE, file_size - start)
        range_header = f"bytes={start}-{start + length - 1}"
        response = s3.get_object(Bucket=BUCKET, Key=KEY, Range=range_header)
        raw_bytes = response['Body'].read()

        if start + length < file_size:
            last_newline = raw_bytes.rfind(b'\n')
            if last_newline != -1:
                raw_bytes = raw_bytes[:last_newline]
                length = last_newline + 1

        chunk_text = raw_bytes.decode('utf-8')
        chunks_data.append(mapper(chunk_text))
        start += length
        chunk_num += 1

    results = reducer(chunks_data)


    def format_key(k):
        if isinstance(k[1], str):
            return f"{k[0]}|{k[1]}"
        else:
            return f"{k[0]}|{','.join(k[1])}"


    serializable_results = {format_key(k): v for k, v in results.items()}

    with open("mr_results.json", "w", encoding="utf-8") as f:
        json.dump(serializable_results, f, ensure_ascii=False, indent=2)

    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)
    for k, v in sorted_results[:5]:
        item_str = k[1] if isinstance(k[1], str) else ', '.join(k[1])
        print(f"{k[0]}: {item_str} -> {v}")