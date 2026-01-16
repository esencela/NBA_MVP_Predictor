-- Create seperate schema for stats and predictions
CREATE SCHEMA IF NOT EXISTS stats;
CREATE SCHEMA IF NOT EXISTS predictions;

-- Passwords have been omitted
CREATE ROLE nba_mvp_etl WITH LOGIN PASSWORD '<ETL_PASSWORD>';
CREATE ROLE nba_mvp_ml WITH LOGIN PASSWORD '<ML_PASSWORD>';

-- Set privileges to etl in database
GRANT USAGE, CREATE ON SCHEMA stats TO nba_mvp_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stats TO nba_mvp_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA stats
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO nba_mvp_etl;
ALTER SCHEMA stats OWNER TO nba_mvp_etl;

-- Set privileges to ml in database
GRANT USAGE ON SCHEMA stats TO nba_mvp_ml;
GRANT SELECT ON ALL TABLES IN SCHEMA stats TO nba_mvp_ml;
ALTER DEFAULT PRIVILEGES IN SCHEMA stats
GRANT SELECT ON TABLES TO nba_mvp_ml;

GRANT USAGE, CREATE ON SCHEMA predictions TO nba_mvp_ml;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA predictions TO nba_mvp_ml;
ALTER DEFAULT PRIVILEGES IN SCHEMA predictions
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO nba_mvp_ml;
ALTER SCHEMA predictions OWNER TO nba_mvp_ml;