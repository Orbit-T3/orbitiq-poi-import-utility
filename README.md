# POI Importer for OrbitIQ Project

This project provides two methods for extracting and processing Points of Interest (POI) data for Thailand from Overture Maps:

## Overview

The pipeline:

- Extracts Thailand POI data from Overture Maps S3 bucket
- Filters for operating businesses with high confidence
- Spatially joins with Thai administrative boundaries
- Loads processed data into PostgreSQL database

## Prerequisites

### Required Software

- **DuckDB** - For spatial data processing and parquet handling
- **Python 3.8+** - For automated script
- **PostgreSQL** - Target database (for automated script)
- **Git** - For version control

### Python Dependencies

```bash
pip install duckdb psycopg2-binary
```

### Required Files

- `source-data.csv` - Thai administrative boundaries (tambon/amphoe/changwat with centroids) [@codesanook/thailand-administrative-division-province-district-subdistrict-sql](https://github.com/codesanook/thailand-administrative-division-province-district-subdistrict-sql)
- DuckDB CLI executable

---

## Getting Started

### Clone the Repository

```bash
git clone <repository-url>
cd orbitiq-poi-import-utility
```

### File Structure

```
orbitiq-poi-import-utility/
├── README.md              # This file
├── script.py              # Automated Python script
├── source-data.csv        # Thai admin boundaries (required for script)
├── th_poi_data.geoparquet # Output from manual method
└── data/                  # Partitioned parquet files
    ├── basic_category=restaurant/
    ├── basic_category=hotel/
    └── ...
```

## Part 1: DuckDB Process

1. **Run DuckDB**

    ```bash
    duckdb
    ```

2. **Start UI (optional)**

    ```sql
    CALL start_ui();
    ```

3. **Install and Load Extensions**

    ```sql
     INSTALL spatial;
     INSTALL httpfs;
     INSTALL postgres;

     LOAD spatial;
     LOAD httpfs;
     LOAD postgres;
    ```

4. **Configure S3 and Get Latest Version**

    ```sql
    SET s3_region = 'us-west-2';
    SET VARIABLE latest_version = (
        SELECT latest FROM 'https://stac.overturemaps.org/catalog.json'
    );
    ```

5. **Create Thailand Boundary Table**

    ```sql
     CREATE OR REPLACE TABLE thailand_boundary AS
     SELECT
     id,
     names.primary AS name,
     subtype,
     country,
     geometry
     FROM
     read_parquet(
         's3://overturemaps-us-west-2/release/' || getvariable('latest_version') || '/theme=divisions/type=division_area/*',
         hive_partitioning = 1
     )
     WHERE
     country = 'TH'
     AND subtype = 'country'
     LIMIT 1;
    ```

6. **Extract and Filter POI Data**
    ```sql
     COPY (
     SELECT
         p.id,
         p.names.primary           AS name,
         p.basic_category,
         p.confidence,
         p.addresses[1].region     AS province,
         p.addresses[1].locality   AS city,
         p.addresses[1].freeform   AS address,
         p.addresses[1].postcode   AS postcode,
         ST_X(p.geometry)          AS longitude,
         ST_Y(p.geometry)          AS latitude,
         ST_AsWKB(p.geometry)      AS geometry
     FROM
         read_parquet(
         's3://overturemaps-us-west-2/release/' || getvariable('latest_version') || '/theme=places/type=place/*',
         filename = true,
         hive_partitioning = 1
         ) AS p,
         thailand_boundary_union AS th
     WHERE
         p.operating_status = 'open'
         AND p.confidence >= 0.7
         AND p.basic_category IS NOT NULL
         AND p.names.primary IS NOT NULL
         AND length(trim(p.names.primary)) >= 2
         AND list_contains(list_transform(p.addresses, x -> x.country), 'TH')
         AND ST_Within(p.geometry, th.geometry)
         AND NOT regexp_matches(p.names.primary, '^[•·▪▫◆◇○●★☆♥♡✅📍📌➤➔\-#@\s]+')
     ) TO 'th_poi_data.parquet' (FORMAT 'parquet', COMPRESSION 'snappy');
    ```
7. **Categorize the basic_category**

The script designed for path like `data/{$tagId}` so you need to create a folder by tagId based on current tagId on your database.

Execute the script to auto categorize based on your parquet file from earlier:

```base
python ./categorize.py
```

So is this a definition of `tagId`:
| tagId | Name |
|--------------|---------------|
| 1 | แหล่งที่พักขนาดใหญ่ ( Large residential ) |
| 2 | สถานศึกษา (education) |
| 3 | หน่วยงานราชการ (Government agencies ) |
| 4 | ออฟฟิศให้เช่า ( Business Center ) |
| 5 | สาธารณสุข (healthcare) |
| 6 | แหล่งรวมร้านค้าและบริการ ( Shopping Area ) |
| 7 | การเงิน (financial) |
| 8 | ที่พัก ( Hotel ) |
| 9 | ศาสนา (community_religion) |
| 10 | สาขาของขนส่ง (Postal Branch) |
| 11 | คู่แข่ง (competitor) |

### Output Files

- `th_poi_data.geoparquet` - Complete filtered POI dataset
- `data/{$tagId}/basic_category={$name}` - Directory containing partitioned parquet files by category

---

## Part 2: Automated Python Script

1. **Ensure prerequisites are met**
    - PostgreSQL is running on localhost:5432
    - Database `orbitiq` exists
    - `source-data.csv` is in the current directory
    - `data/` directory contains partitioned parquet files

2. **Update database connection if needed**
   Edit line 69 in `script.py`:

    ```python
    "host=localhost port=5432 dbname=orbitiq user=postgres password=1122"
    ```

3. **Execute the script**
    ```bash
    python script.py
    ```

### Script Workflow

1. **Phase 1**: Stream DuckDB results to PostgreSQL staging table using COPY protocol
2. **Phase 2**: Upsert unique sub-tags from staging data
3. **Phase 3**: Insert new unique locations (deduplicated by coordinates)
4. **Phase 4**: Insert POIs with resolved foreign keys

### Performance Notes

- Uses batch processing (100,000 rows per COPY operation)
- Spatial indexing for efficient admin boundary joins
- Server-side deduplication to reduce data transfer
- Transaction-based processing for data integrity

---

## Data Sources

- **Overture Maps**: https://overturemaps.org/
- **Thai Administrative Boundaries**: Custom CSV file with tambon/amphoe/changwat data [@codesanook/thailand-administrative-division-province-district-subdistrict-sql](https://github.com/codesanook/thailand-administrative-division-province-district-subdistrict-sql)

## License

This project follows the same license as the Overture Maps data source
