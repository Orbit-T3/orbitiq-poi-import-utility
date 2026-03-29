import csv
import io

import duckdb
import psycopg2
import psycopg2.extras

# How many DuckDB rows to buffer per COPY call.
# Larger = fewer round-trips but more RAM. 100k is a safe default.
COPY_BATCH = 100_000

# ── DuckDB: read parquet files and resolve Thai admin boundaries ──────────────
# DuckDB is used purely for reading/transforming the source data locally.
# It never writes to Postgres directly; it just produces rows for us to stream.

duck = duckdb.connect()
duck.execute("INSTALL spatial; LOAD spatial;")

# Load the Thai admin CSV (tambon/amphoe/changwat with lat/lon centroids)
# into an in-memory DuckDB table so we can spatial-join against it.
duck.execute("""
    CREATE OR REPLACE TEMP TABLE thai_admin AS
    SELECT * FROM read_csv_auto('source-data.csv');
""")

# Main query:
#   1. Read all parquet files; extract category name and tag ID from the file path.
#   2. Spatial-join each POI to the nearest Thai admin row within a 0.5° bounding box.
#   3. Clean up province/district/subdistrict prefixes (จ./อ./ต.).
rel = duck.execute("""
    WITH extracted_data AS (
        SELECT
            *,
            (
                upper(substring(replace(regexp_extract(filename, 'basic_category=([^/\\\\]+)', 1), '_', ' '), 1, 1))
                || lower(substring(replace(regexp_extract(filename, 'basic_category=([^/\\\\]+)', 1), '_', ' '), 2))
            ) AS folder_subtag,
            regexp_extract(filename, '[/\\\\]data[/\\\\]tagid=(\\d+)[/\\\\]', 1)::INT AS folder_tagid,
            address AS addr,
            regexp_replace(postcode, '[\r\n\t ]+', '') AS postcode,
            ST_Point(longitude, latitude) AS geom
        FROM read_parquet('.\\data\\*\\*\\*.parquet', filename = true)
        WHERE regexp_extract(filename, 'basic_category=([^/\\\\]+)', 1) IS NOT NULL
            AND latitude IS NOT NULL
            AND longitude IS NOT NULL
    )
    SELECT
        e.name,
        e.folder_subtag,
        e.folder_tagid,
        e.latitude::DECIMAL(10,7)  AS lat,
        e.longitude::DECIMAL(10,7) AS lon,
        split_part(trim(b.CHANGWAT_T), ' ', 2) AS province,
        split_part(trim(b.AMPHOE_T),  ' ', 2) AS district,
        split_part(trim(b.TAMBON_T),  ' ', 2) AS subdistrict,
        e.addr AS address,
        e.postcode
    FROM extracted_data e
    LEFT JOIN LATERAL (
        SELECT CHANGWAT_T, AMPHOE_T, TAMBON_T
        FROM thai_admin t
        WHERE t.LAT  BETWEEN e.latitude  - 0.5 AND e.latitude  + 0.5
          AND t.LONG BETWEEN e.longitude - 0.5 AND e.longitude + 0.5
        ORDER BY ST_Distance(e.geom, ST_Point(t.LONG, t.LAT))
        LIMIT 1
    ) b ON true;
""")

# ── PostgreSQL setup ──────────────────────────────────────────────────────────
pg = psycopg2.connect(
    "host=localhost port=5432 dbname=orbitiq user=postgres password=1122"
)
pg.autocommit = False
pg_cur = pg.cursor()

# ── Ensure location table has correct column types ────────────────────────────
# Safe to run every time:
#   - ADD COLUMN IF NOT EXISTS won't fail if loc_geom already exists.
#   - ALTER COLUMN TYPE updates precision on loc_latitude / loc_longitude.
pg_cur.execute("""
    ALTER TABLE location
        ALTER COLUMN loc_latitude  TYPE NUMERIC(10, 7),
        ALTER COLUMN loc_longitude TYPE NUMERIC(10, 7);
""")
pg_cur.execute("""
    ALTER TABLE location
        ADD COLUMN IF NOT EXISTS loc_geom     GEOMETRY(Point, 4326),
        ADD COLUMN IF NOT EXISTS loc_zip_code TEXT;
""")
pg_cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_location_geom
        ON location USING GIST(loc_geom);
""")
pg.commit()
print("[setup] location table columns verified/updated")

# Temporary staging table — holds raw rows from DuckDB for this session only.
# All deduplication and FK resolution happens server-side via SQL after staging.
# NUMERIC(10,7) = 7 decimal places → ~1cm coordinate precision.
pg_cur.execute("""
    CREATE TEMP TABLE _staging (
        poi_name    TEXT,
        subtag      TEXT,
        tagid       INT,
        lat         NUMERIC(10, 7),
        lon         NUMERIC(10, 7),
        province    TEXT,
        district    TEXT,
        subdistrict TEXT,
        address     TEXT,
        postcode    TEXT
    ) ON COMMIT PRESERVE ROWS
""")

# ── Phase 1: Stream DuckDB results → Postgres staging via COPY ────────────────
# COPY is the fastest bulk-load path in Postgres — it bypasses the SQL parser
# and skips per-row overhead entirely. We chunk to bound memory usage.
total_staged = 0
while True:
    batch = rel.fetchmany(COPY_BATCH)
    if not batch:
        break

    buf = io.StringIO()
    csv.writer(buf).writerows(batch)
    buf.seek(0)
    pg_cur.copy_expert("COPY _staging FROM STDIN WITH CSV", buf)
    total_staged += len(batch)
    print(f"[COPY] {total_staged:,} rows staged...")

pg.commit()
print(f"[COPY] Done — {total_staged:,} rows in staging table")

# ── Phase 2: Upsert sub_tags ──────────────────────────────────────────────────
# One server-side INSERT from DISTINCT staging rows.
# ON CONFLICT skips names that already exist in sub_tag.
pg_cur.execute("""
    INSERT INTO sub_tag (stg_name, stg_tag_id, stg_updated_at, stg_created_at)
    SELECT DISTINCT subtag, tagid, now(), now()
    FROM _staging
    ON CONFLICT (stg_name) DO NOTHING
""")
print(f"[sub_tag] {pg_cur.rowcount:,} inserted")

# ── Phase 3: Insert new locations ────────────────────────────────────────────
# DISTINCT ON (lat, lon) collapses duplicate coordinates from staging.
# NOT EXISTS skips coordinates that are already in the location table.
# ST_SetSRID(ST_MakePoint(lon, lat), 4326) populates the PostGIS geometry column.
pg_cur.execute("""
    INSERT INTO location
        (loc_province, loc_district, loc_sub_district,
         loc_latitude, loc_longitude, loc_geom,
         loc_updated_at, loc_created_at, loc_address, loc_zip_code)
    SELECT DISTINCT ON (lat, lon)
        province, district, subdistrict,
        lat, lon,
        ST_SetSRID(ST_MakePoint(lon, lat), 4326),
        now(), now(), address, postcode
    FROM _staging s
    WHERE NOT EXISTS (
        SELECT 1 FROM location l
        WHERE l.loc_latitude  = s.lat
          AND l.loc_longitude = s.lon
    )
""")
print(f"[location] {pg_cur.rowcount:,} inserted")

# ── Phase 4: Insert POIs via JOIN ─────────────────────────────────────────────
# JOIN resolves sub_tag and location IDs server-side — no Python lookup needed.
# DISTINCT ON (loc_id) enforces the one-POI-per-location rule.
# NOT EXISTS skips any location that already has a POI.
pg_cur.execute("""
    INSERT INTO place_of_interest
        (poi_name, poi_tag_id, poi_subtag_id, poi_loc_id, poi_is_deleted, poi_request)
    SELECT DISTINCT ON (l.loc_id)
        s.poi_name, s.tagid, st.stg_id, l.loc_id, false, false
    FROM _staging s
    JOIN sub_tag  st ON st.stg_name     = s.subtag
    JOIN location l  ON l.loc_latitude  = s.lat
                    AND l.loc_longitude = s.lon
    WHERE NOT EXISTS (
        SELECT 1 FROM place_of_interest p WHERE p.poi_loc_id = l.loc_id
    )
""")
print(f"[poi] {pg_cur.rowcount:,} inserted")

# ── Commit and close ──────────────────────────────────────────────────────────
pg.commit()
pg_cur.close()
pg.close()
duck.close()
print(f"Done! {total_staged:,} rows processed")