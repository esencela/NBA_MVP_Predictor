-- Grant connect privileges to all users
GRANT CONNECT ON DATABASE nba_mvp TO etl_user;
GRANT CONNECT ON DATABASE nba_mvp TO ml_user;
GRANT CONNECT ON DATABASE nba_mvp TO app_user;

-- Set privileges to etl in database
-- Stats Schema
GRANT USAGE, CREATE ON SCHEMA stats TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA stats TO etl_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA stats
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO etl_user;
ALTER SCHEMA stats OWNER TO etl_user;

-- Metadata Schema
GRANT USAGE, CREATE ON SCHEMA metadata TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON metadata.data_freshness TO etl_user;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA metadata TO etl_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA metadata
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO etl_user;

ALTER TABLE metadata.data_freshness OWNER TO etl_user;

-- Set privileges to ml in database
-- Stats Schema
GRANT USAGE ON SCHEMA stats TO ml_user;
GRANT SELECT ON ALL TABLES IN SCHEMA stats TO ml_user;

-- etl_user creates tables in stats schema, so we need to grant select privileges to ml_user for those tables
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA stats
GRANT SELECT ON TABLES TO ml_user;

-- Predictions Schema
GRANT USAGE, CREATE ON SCHEMA predictions TO ml_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA predictions TO ml_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA predictions
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO ml_user;

ALTER SCHEMA predictions OWNER TO ml_user;

-- Serving Schema
GRANT USAGE, CREATE ON SCHEMA serving TO ml_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA serving TO ml_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA serving
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO ml_user;

ALTER SCHEMA serving OWNER TO ml_user;

-- Set privileges to app in database
-- Serving Schema
GRANT USAGE ON SCHEMA serving TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO app_user;

-- ml_user creates tables in serving schema, so we need to grant select privileges to app_user for those tables
ALTER DEFAULT PRIVILEGES FOR ROLE ml_user IN SCHEMA serving
GRANT SELECT ON TABLES TO app_user;

-- Metadata Schema
GRANT USAGE ON SCHEMA metadata TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA metadata TO app_user;

-- ml_user creates tables in metadata schema, so we need to grant select privileges to app_user for those tables
ALTER DEFAULT PRIVILEGES FOR ROLE ml_user IN SCHEMA metadata
GRANT SELECT ON TABLES TO app_user;