-- Create seperate schema for stats, predictions and serving
CREATE SCHEMA IF NOT EXISTS stats;
CREATE SCHEMA IF NOT EXISTS predictions;
CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS stats.player_features (
    "Season" INT NOT NULL,
    "Player" VARCHAR(40) NOT NULL,
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

CREATE TABLE IF NOT EXISTS stats.player_stats (
    "Season" INT NOT NULL,
    "Player" VARCHAR(40) NOT NULL,
    "Team" VARCHAR(4) NOT NULL,
    "MP" FLOAT,
    "PTS" FLOAT,
    "AST" FLOAT,
    "TRB" FLOAT,
    "STL" FLOAT,
    "BLK" FLOAT
);

CREATE TABLE IF NOT EXISTS predictions.mvp_predictions (
    "Season" INT NOT NULL,
    "Player" VARCHAR(40) NOT NULL,
    "Predicted_Share" FLOAT
);

CREATE OR REPLACE VIEW serving.leaderboard AS (
SELECT 
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
ON p."Player" = s."Player" 
AND p."Season" = s."Season"
WHERE "Predicted_Share" > 0
ORDER BY "Predicted_Share" DESC);
