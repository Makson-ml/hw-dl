import duckdb
import json

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW';")
con.execute("""
    SET s3_endpoint='127.0.0.1:9000';
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
""")

query = """
WITH raw_data AS (
    SELECT vacancy_id, name, key_skills
    FROM read_json_auto('s3://hh-lake/vacancies.jsonl')
),
normalized_skills AS (
    SELECT 
        vacancy_id,
        COALESCE(CAST(name AS VARCHAR), '') as name,
        CASE 
            WHEN typeof(key_skills) = 'VARCHAR' THEN [CAST(key_skills AS VARCHAR)]
            WHEN typeof(key_skills) = 'VARCHAR[]' THEN key_skills
            ELSE CAST([] AS VARCHAR[])
        END as skills_list
    FROM raw_data
),
filtered AS (
    SELECT * FROM normalized_skills
    WHERE lower(name) LIKE '%golang%' 
       OR lower(name) LIKE '%goland%'
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'go')
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'golang')
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'goland')
),
unnested AS (
    SELECT 
        vacancy_id, 
        lower(trim(CAST(skill AS VARCHAR))) as clean_skill
    FROM filtered, unnest(skills_list) as t(skill)
    WHERE skill IS NOT NULL AND trim(CAST(skill AS VARCHAR)) != ''
),
unique_skills_per_vacancy AS (
    SELECT DISTINCT vacancy_id, clean_skill
    FROM unnested
),
grouped AS (
    SELECT 
        vacancy_id, 
        list_sort(list(clean_skill)) as sorted_skills
    FROM unique_skills_per_vacancy
    GROUP BY vacancy_id
)
SELECT 'S' as type, clean_skill as item, count(*) as cnt 
FROM unique_skills_per_vacancy
GROUP BY clean_skill

UNION ALL

SELECT 'P' as type, s1 || ',' || s2 as item, count(*) as cnt
FROM grouped, unnest(sorted_skills) as t1(s1), unnest(sorted_skills) as t2(s2)
WHERE s1 < s2
GROUP BY s1, s2

UNION ALL

SELECT 'T' as type, s1 || ',' || s2 || ',' || s3 as item, count(*) as cnt
FROM grouped, unnest(sorted_skills) as t1(s1), unnest(sorted_skills) as t2(s2), unnest(sorted_skills) as t3(s3)
WHERE s1 < s2 AND s2 < s3
GROUP BY s1, s2, s3

ORDER BY type, cnt DESC;
"""

print("Выполняю запрос в DuckDB...")
df = con.execute(query).fetchdf()

duckdb_results = {}
for _, row in df.iterrows():
    duckdb_results[f"{row['type']}|{row['item']}"] = int(row['cnt'])

with open("duckdb_results.json", "w", encoding="utf-8") as f:
    json.dump(duckdb_results, f, ensure_ascii=False, indent=2)

with open("mr_results.json", "r", encoding="utf-8") as f:
    mr_data = json.load(f)

if mr_data == duckdb_results:
    print("РЕЗУЛЬТАТЫ MAPREDUCE И DUCKDB ПОЛНОСТЬЮ СОВПАДАЮТ")
else:
    print("ОБНАРУЖЕНЫ РАСХОЖДЕНИЯ")
    mismatches = []
    for k in mr_data:
        if k in duckdb_results and mr_data[k] != duckdb_results[k]:
            mismatches.append(f"{k} | MR: {mr_data[k]} | DuckDB: {duckdb_results[k]}")
    print(f"Несовпадений по числам: {len(mismatches)}")
    for m in mismatches[:5]:
        print(f"   -> {m}")