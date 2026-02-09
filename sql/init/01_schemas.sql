-- Create seperate schema for stats, predictions and serving
CREATE SCHEMA IF NOT EXISTS stats;
CREATE SCHEMA IF NOT EXISTS predictions;
CREATE SCHEMA IF NOT EXISTS serving;
CREATE SCHEMA IF NOT EXISTS metadata;

-- Table holding scaled features for training MVP model and making predictions
CREATE TABLE IF NOT EXISTS stats.player_features (
    "Season" INT NOT NULL,
    "player_id" VARCHAR(9) NOT NULL,
    "Player" TEXT NOT NULL,
    "MP" FLOAT NOT NULL,
    "PTS" FLOAT,
    "AST" FLOAT,
    "TRB" FLOAT,
    "STL" FLOAT,
    "BLK" FLOAT,
    "TS%" FLOAT,
    "PER" FLOAT,
    "WS" FLOAT,
    "BPM" FLOAT,
    "VORP" FLOAT,
    "USG%" FLOAT,
    "VORP_W/L" FLOAT,
    "W/L%" FLOAT,
    "Share" FLOAT
);

-- Table holding cleaned stats for player seasons
CREATE TABLE IF NOT EXISTS stats.player_stats (
    "Season" INT NOT NULL,
    "player_id" VARCHAR(9) NOT NULL,
    "Player" TEXT NOT NULL,
    "Team" VARCHAR(3) NOT NULL,
    "MP" FLOAT,
    "PTS" FLOAT,
    "AST" FLOAT,
    "TRB" FLOAT,
    "STL" FLOAT,
    "BLK" FLOAT
);

-- Table holding MVP predictions for each player in current season
CREATE TABLE IF NOT EXISTS predictions.mvp_predictions (
    "Season" INT NOT NULL,
    "player_id" VARCHAR(9) NOT NULL,
    "Player" TEXT NOT NULL,
    "Predicted_Share" FLOAT
);

-- View to show leaderboard of MVP predicted share. Only contains players with predicted share > 0
CREATE OR REPLACE VIEW serving.leaderboard AS (
SELECT 
    s."player_id",
    s."Player",
    s."Team", 
    s."MP", 
    s."PTS", 
    s."AST", 
    s."TRB", 
    s."STL", 
    s."BLK", 
    p."Predicted_Share"
FROM predictions.mvp_predictions AS p
JOIN stats.player_stats AS s
ON p."player_id" = s."player_id" 
AND p."Season" = s."Season"
WHERE "Predicted_Share" > 0
ORDER BY "Predicted_Share" DESC);

-- Table tracking updates to mvp predictions
CREATE TABLE IF NOT EXISTS metadata.update_runs (
    update_id SERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    status TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    error_message TEXT
);