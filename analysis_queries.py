#B''SD



#FOR THE AUTHORATATIVE EXPLANATION OF WHAT'S GOING ON HERE...Check the sql_explanation_and_output.txt file
'''
For the authoarative explanaiton of hwat's going on here
Check the sql_explanation_and_output.txt file, should tell you what's going on
in each method. The nice thing about sql is it fairly legible even to the 
uninitiated. 
'''
import os, sqlalchemy as sa
from dotenv import load_dotenv
load_dotenv()

eng = sa.create_engine(os.getenv("DB_URL")) # GETTING my link from db

def run_sql_file(conn, path):  #use this to open/read my haversine.sql file
    with open(path, "r", encoding="utf-8") as f:
        conn.execute(sa.text(f.read()))


Q_SHORTEST = """
WITH one_trip_per_route AS (
  SELECT DISTINCT ON (route_id) route_id, trip_id
  FROM trips ORDER BY route_id, trip_id
), ordered AS (
  SELECT r.route_id, st.trip_id, st.stop_sequence, s.stop_lat, s.stop_lon,
         LEAD(s.stop_lat) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS next_lat,
         LEAD(s.stop_lon) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS next_lon
  FROM stop_times st
  JOIN one_trip_per_route r ON r.trip_id = st.trip_id
  JOIN stops s ON s.stop_id = st.stop_id
), legs AS (
  SELECT route_id, haversine_km(stop_lat, stop_lon, next_lat, next_lon) AS leg_km
  FROM ordered WHERE next_lat IS NOT NULL
)
SELECT ro.route_id, COALESCE(ro.route_short_name, ro.route_long_name) AS route_name,
       ROUND(SUM(leg_km)::numeric, 2) AS total_km
FROM legs l JOIN routes ro USING (route_id)
GROUP BY ro.route_id, route_name
ORDER BY total_km ASC
LIMIT 10;
"""

Q_LONGEST_LEG = """
WITH one_trip_per_route AS (
  SELECT DISTINCT ON (route_id) route_id, trip_id
  FROM trips ORDER BY route_id, trip_id
), ordered AS (
  SELECT r.route_id, st.trip_id, st.stop_sequence, s.stop_lat, s.stop_lon,
         LEAD(s.stop_lat) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS next_lat,
         LEAD(s.stop_lon) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS next_lon
  FROM stop_times st
  JOIN one_trip_per_route r ON r.trip_id = st.trip_id
  JOIN stops s ON s.stop_id = st.stop_id
), legs AS (
  SELECT route_id, haversine_km(stop_lat, stop_lon, next_lat, next_lon) AS leg_km
  FROM ordered WHERE next_lat IS NOT NULL
)
SELECT ro.route_id, COALESCE(ro.route_short_name, ro.route_long_name) AS route_name,
       ROUND(MAX(leg_km)::numeric, 2) AS longest_leg_km
FROM legs l JOIN routes ro USING (route_id)
GROUP BY ro.route_id, route_name
ORDER BY longest_leg_km DESC
LIMIT 10;
"""

Q_AVG_SPACING = """
WITH one_trip_per_route AS (
  SELECT DISTINCT ON (route_id) route_id, trip_id
  FROM trips ORDER BY route_id, trip_id
), ordered AS (
  SELECT r.route_id, st.trip_id, st.stop_sequence, s.stop_lat, s.stop_lon,
         LEAD(s.stop_lat) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS next_lat,
         LEAD(s.stop_lon) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS next_lon
  FROM stop_times st
  JOIN one_trip_per_route r ON r.trip_id = st.trip_id
  JOIN stops s ON s.stop_id = st.stop_id
), legs AS (
  SELECT route_id, haversine_km(stop_lat, stop_lon, next_lat, next_lon) AS leg_km
  FROM ordered WHERE next_lat IS NOT NULL
), route_avg AS (
  SELECT route_id, AVG(leg_km) AS avg_leg_km
  FROM legs GROUP BY route_id
)
SELECT ro.route_id, COALESCE(ro.route_short_name, ro.route_long_name) AS route_name,
       ROUND(avg_leg_km::numeric, 3) AS avg_km_between_stops
FROM route_avg ra JOIN routes ro USING (route_id)
ORDER BY avg_km_between_stops ASC
LIMIT 10;
"""

with eng.begin() as c: #opening path to db
  print(eng)
  '''
  Initially GPT wanted me to like load haversine.sql content into the command line
  or something, but i don't understand sql that well, I wanted it present, so 
  we're using this
  '''
  run_sql_file(c, "haversine.sql")  #runining .sql file
  #reading the fields
  for label, q in [("Shortest routes", Q_SHORTEST),
                   ("Longest single leg", Q_LONGEST_LEG),
                   ("Avg spacing", Q_AVG_SPACING)]:
    print(f"\n=== {label} ===") #fancy printing and sql stuff I'm now learning for the first time
    for row in c.execute(sa.text(q)):
      print(dict(row._mapping))
