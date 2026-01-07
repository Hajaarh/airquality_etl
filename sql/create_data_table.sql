CREATE TABLE IF NOT EXISTS airq_data.air_quality_history (
  city STRING,
  country STRING,
  date DATE,
  pm10 FLOAT64,
  pm2_5 FLOAT64,
  carbon_monoxide FLOAT64,
  nitrogen_dioxide FLOAT64,
  sulphur_dioxide FLOAT64,
  ozone FLOAT64,
  european_aqi FLOAT64,
  population INT64,
  latitude FLOAT64,
  longitude FLOAT64
);
