#B''SD
import os
import sys
import zipfile
import pandas as pd
from sqlalchemy import create_engine, types, text

"""
This was heavily GPT generated, I don't touch sql...

Load the essential WMATA GTFS files into AWS RDS Postgres...

To RUN:
  #my values are in the .env
  export DB_URL="postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}?sslmode=require"
  #need your own file path of course
  python load_gtfs_to_rds.py /path/to/bus-gtfs-static.zip 
Notes:
  * only leads routes.txt, stops.txt, trips.txt, stop_times.txt, I think we only need those and saves time and space
  * Keeps GTFS times as TEXT (they can exceed 24h) (So says GPT)
  * Creates/overwrites tables (if_exists='replace') for PoC convenience
"""

REQUIRED_FILES = {"routes.txt", "stops.txt", "trips.txt", "stop_times.txt"}

DTYPES = {
    "routes.txt": {
        "route_id": "string",
        "route_short_name": "string",
        "route_long_name": "string",
    },
    "stops.txt": {
        "stop_id": "string",
        "stop_name": "string",
        "stop_lat": "float64",
        "stop_lon": "float64",
    },
    "trips.txt": {
        "route_id": "string",
        "service_id": "string",
        "trip_id": "string",
    },
    "stop_times.txt": {
        "trip_id": "string",
        "arrival_time": "string",     # keep as TEXT due to >24h times
        "departure_time": "string",
        "stop_id": "string",
        "stop_sequence": "Int64",     # nullable integer
    },
}

SQL_DTYPES = {
    "routes.txt": {
        "route_id": types.Text(),
        "route_short_name": types.Text(),
        "route_long_name": types.Text(),
    },
    "stops.txt": {
        "stop_id": types.Text(),
        "stop_name": types.Text(),
        "stop_lat": types.Float(),
        "stop_lon": types.Float(),
    },
    "trips.txt": {
        "route_id": types.Text(),
        "service_id": types.Text(),
        "trip_id": types.Text(),
    },
    "stop_times.txt": {
        "trip_id": types.Text(),
        "arrival_time": types.Text(),
        "departure_time": types.Text(),
        "stop_id": types.Text(),
        "stop_sequence": types.Integer(),
    },
}

def main():
    if len(sys.argv) < 2:
        print("Usage: python load_gtfs_to_rds.py /path/to/bus-gtfs-static.zip [schema_name]")
        sys.exit(1)

    zip_path = sys.argv[1]
    schema = sys.argv[2] if len(sys.argv) > 2 else "public"

    db_url = os.getenv("DB_URL")
    if not db_url:
        print("ERROR: Set DB_URL env var, e.g.:")
        print('  export DB_URL="postgresql+psycopg2://USER:PASS@RDS_ENDPOINT:5432/DBNAME?sslmode=require"')
        sys.exit(2)

    engine = create_engine(db_url)

    # Ensure schema exists (safe no-op if it already does)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

    with zipfile.ZipFile(zip_path, "r") as z:
        zip_names = {info.filename.split("/")[-1] for info in z.infolist()}
        missing = REQUIRED_FILES - zip_names
        if missing:
            print(f"ERROR: ZIP is missing required files: {sorted(missing)}")
            sys.exit(3)

        for info in z.infolist():
            base = info.filename.split("/")[-1]
            if base not in REQUIRED_FILES:
                continue

            table = base.replace(".txt", "")
            print(f"Loading {base} -> {schema}.{table} ...")

            with z.open(info) as f:
                df = pd.read_csv(
                    f,
                    dtype=DTYPES.get(base, None),
                    keep_default_na=False,  # keep empty strings as '', not NaN
                )

            # Trim to expected columns (some feeds add extras)
            expected_cols = list(SQL_DTYPES[base].keys())
            missing_cols = [c for c in expected_cols if c not in df.columns]
            if missing_cols:
                raise ValueError(f"{base} missing expected columns: {missing_cols}")
            df = df[expected_cols]

            # Write to Postgres
            df.to_sql(
                table,
                engine,
                schema=schema,
                if_exists="replace",   # For PoC; use 'append' later if you prefer
                index=False,
                chunksize=5000,
                method="multi",
                dtype=SQL_DTYPES[base],
            )

            print(f"✓ Loaded {len(df):,} rows into {schema}.{table}")

    print("\nAll done. Tables created:")
    for base in sorted(REQUIRED_FILES):
        print(f'  {schema}.{base.replace(".txt","")}')
    print("\nNext step: wire Bedrock to generate SQL and summarize results (we’ll do that separately).")

if __name__ == "__main__":
    main()
