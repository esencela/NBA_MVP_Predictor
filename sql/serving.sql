-- Create schema for serving
CREATE SCHEMA IF NOT EXISTS serving;

CREATE OR REPLACE VIEW serving.leaderboard AS
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
FROM predictions.player_predictions AS p
JOIN stats.player_stats AS s
ON p."Player" = s."Player" 
AND p."Season" = s."Season"
WHERE "Predicted_Share" > 0
ORDER BY "Predicted_Share" DESC;