-- a Function that returns distance in km
CREATE OR REPLACE FUNCTION haversine_km(
  lat1 DOUBLE PRECISION, lon1 DOUBLE PRECISION,
  lat2 DOUBLE PRECISION, lon2 DOUBLE PRECISION
)
RETURNS DOUBLE PRECISION AS $$
DECLARE
  r CONSTANT DOUBLE PRECISION := 6371.0; -- earth radius in km, who knew?
BEGIN
  RETURN 2 * r * asin(
    sqrt(
      sin(radians(lat2 - lat1) / 2)^2
      + cos(radians(lat1))
      * cos(radians(lat2))
      * sin(radians(lon2 - lon1) / 2)^2
    )
  );
END;
$$ LANGUAGE plpgsql IMMUTABLE;