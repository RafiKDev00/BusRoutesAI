import os, sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()
engine = sa.create_engine(os.getenv("DB_URL"))

with engine.connect() as conn:
    result = conn.execute(sa.text("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_name IN ('routes','trips','stops','stop_times')
        ORDER BY table_name, ordinal_position;
    """))
    print("\nColumns found:\n")
    for row in result:
        print(f"{row.table_name:<12} | {row.column_name}")
