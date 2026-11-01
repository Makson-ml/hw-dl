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

print("=== ЭТАП 5: ВЛИЯНИЕ НАВЫКОВ НА ЗАРПЛАТУ ===")

salary_query = """
WITH raw AS (
    SELECT vacancy_id, name, key_skills, salary
    FROM read_json_auto('s3://hh-lake/vacancies.jsonl')
),
norm AS (
    SELECT vacancy_id, name,
        CASE WHEN typeof(key_skills) = 'VARCHAR' THEN [CAST(key_skills AS VARCHAR)]
             WHEN typeof(key_skills) = 'VARCHAR[]' THEN key_skills
             ELSE CAST([] AS VARCHAR[]) END as skills_list,
        salary
    FROM raw
),
filtered AS (
    SELECT * FROM norm
    WHERE lower(name) LIKE '%golang%' OR lower(name) LIKE '%goland%'
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'go')
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'golang')
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'goland')
),
with_sal AS (
    SELECT vacancy_id, skills_list,
        CASE 
            WHEN salary IS NOT NULL AND salary.from IS NOT NULL AND salary.to IS NOT NULL THEN (CAST(salary.from AS INT) + CAST(salary.to AS INT)) / 2
            WHEN salary IS NOT NULL AND salary.from IS NOT NULL THEN CAST(salary.from AS INT)
            WHEN salary IS NOT NULL AND salary.to IS NOT NULL THEN CAST(salary.to AS INT)
            ELSE NULL END as avg_sal
    FROM filtered WHERE salary IS NOT NULL
),
unnested AS (
    SELECT DISTINCT vacancy_id, avg_sal, lower(trim(CAST(skill AS VARCHAR))) as skill
    FROM with_sal, unnest(skills_list) as t(skill)
    WHERE skill IS NOT NULL AND trim(CAST(skill AS VARCHAR)) != ''
),
base AS (
    SELECT skill, MEDIAN(avg_sal) as med_sal, COUNT(*) as cnt
    FROM unnested GROUP BY skill HAVING COUNT(*) >= 10 ORDER BY cnt DESC LIMIT 1
),
pairs AS (
    SELECT u1.skill as s1, u2.skill as s2, MEDIAN(u1.avg_sal) as pair_med, COUNT(*) as p_cnt
    FROM unnested u1 JOIN unnested u2 ON u1.vacancy_id = u2.vacancy_id AND u1.skill < u2.skill
    GROUP BY u1.skill, u2.skill HAVING COUNT(*) >= 3
)
SELECT b.skill as base_skill, b.med_sal as base_median, 
       CASE WHEN p.s1 = b.skill THEN p.s2 ELSE p.s1 END as add_skill,
       p.pair_med as pair_median, (p.pair_med - b.med_sal) as diff
FROM base b JOIN pairs p ON p.s1 = b.skill OR p.s2 = b.skill
ORDER BY diff DESC;
"""

df_sal = con.execute(salary_query).fetchdf()
print(df_sal.to_string(index=False))
df_sal.to_csv("salary_impact.csv", index=False)

print("\n=== ЭТАП 6: РЕГИОНАЛЬНЫЙ АНАЛИЗ ===")

region_query = """
WITH raw AS (
    SELECT vacancy_id, name, key_skills, salary, area
    FROM read_json_auto('s3://hh-lake/vacancies.jsonl')
),
norm AS (
    SELECT vacancy_id, name,
        CASE WHEN typeof(key_skills) = 'VARCHAR' THEN [CAST(key_skills AS VARCHAR)]
             WHEN typeof(key_skills) = 'VARCHAR[]' THEN key_skills
             ELSE CAST([] AS VARCHAR[]) END as skills_list,
        salary, area.name as region
    FROM raw
),
filtered AS (
    SELECT * FROM norm
    WHERE lower(name) LIKE '%golang%' OR lower(name) LIKE '%goland%'
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'go')
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'golang')
       OR list_contains(list_transform(skills_list, x -> lower(trim(CAST(x AS VARCHAR)))), 'goland')
),
with_sal AS (
    SELECT vacancy_id, region,
        CASE 
            WHEN salary IS NOT NULL AND salary.from IS NOT NULL AND salary.to IS NOT NULL THEN (CAST(salary.from AS INT) + CAST(salary.to AS INT)) / 2
            WHEN salary IS NOT NULL AND salary.from IS NOT NULL THEN CAST(salary.from AS INT)
            WHEN salary IS NOT NULL AND salary.to IS NOT NULL THEN CAST(salary.to AS INT)
            ELSE NULL END as avg_sal
    FROM filtered WHERE salary IS NOT NULL
)
SELECT region, COUNT(*) as vacancies, MEDIAN(avg_sal) as median_salary, AVG(avg_sal) as avg_salary
FROM with_sal
WHERE region IN ('Москва', 'Санкт-Петербург', 'Краснодар')
GROUP BY region
ORDER BY median_salary DESC;
"""

df_reg = con.execute(region_query).fetchdf()
print(df_reg.to_string(index=False))
df_reg.to_csv("region_stats.csv", index=False)

print("\nСохранено в salary_impact.csv и region_stats.csv")