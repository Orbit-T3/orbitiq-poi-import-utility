import duckdb
import json

with open('tags.json', 'r') as f:
    tag_map = json.load(f)

category_to_tagid = {}
for tagid, categories in tag_map.items():
    for cat in categories:
        cat_value = cat.replace("basic_category=", "")
        category_to_tagid[cat_value] = tagid

con = duckdb.connect()

rows = ", ".join(
    f"('{cat}', '{tagid}')" 
    for cat, tagid in category_to_tagid.items()
)

con.execute(f"""
    CREATE TEMP TABLE tag_map AS 
    SELECT * FROM (VALUES {rows}) t(basic_category, tagid)
""")

# Matched - partition by tagid/basic_category
con.execute("""
    COPY (
        SELECT p.*, t.tagid
        FROM read_parquet('th_poi_data.parquet') p
        INNER JOIN tag_map t ON p.basic_category = t.basic_category
        WHERE p.basic_category IS NOT NULL
    )
    TO 'data'
    (FORMAT 'parquet', PARTITION_BY (tagid, basic_category));
""")

# Unmatched - goes to unmatched folder
con.execute("""
    COPY (
        SELECT p.*
        FROM read_parquet('th_poi_data.parquet') p
        LEFT JOIN tag_map t ON p.basic_category = t.basic_category
        WHERE t.basic_category IS NULL
    )
    TO 'data/unmatched'
    (FORMAT 'parquet', PARTITION_BY (basic_category));
""")

print("Done!")