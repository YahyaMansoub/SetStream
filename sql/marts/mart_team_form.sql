-- ==============================================================================
-- mart_team_form.sql
-- Recent team performance metrics: W/L, sets, streaks
-- ==============================================================================

CREATE OR REPLACE VIEW mart_team_form AS
WITH recent_matches AS (
    -- Get matches from last 90 days
    SELECT
        No AS MatchNo,
        DateLocal,
        TeamNameA,
        TeamNameB,
        MatchPointsA,
        MatchPointsB,
        CASE
            WHEN MatchPointsA > MatchPointsB THEN TeamNameA
            WHEN MatchPointsB > MatchPointsA THEN TeamNameB
            ELSE NULL
        END AS Winner
    FROM stg_matches
    WHERE DateLocal >= CURRENT_DATE - INTERVAL 90 DAY
        AND MatchPointsA IS NOT NULL
        AND MatchPointsB IS NOT NULL
),

team_matches AS (
    -- Unpivot to team-level records
    SELECT
        DateLocal,
        MatchNo,
        TeamNameA AS TeamName,
        TeamNameB AS Opponent,
        MatchPointsA AS SetsWon,
        MatchPointsB AS SetsLost,
        CASE WHEN Winner = TeamNameA THEN 1 ELSE 0 END AS Win,
        CASE WHEN Winner IS NOT NULL AND Winner != TeamNameA THEN 1 ELSE 0 END AS Loss
    FROM recent_matches
    WHERE TeamNameA IS NOT NULL
    
    UNION ALL
    
    SELECT
        DateLocal,
        MatchNo,
        TeamNameB AS TeamName,
        TeamNameA AS Opponent,
        MatchPointsB AS SetsWon,
        MatchPointsA AS SetsLost,
        CASE WHEN Winner = TeamNameB THEN 1 ELSE 0 END AS Win,
        CASE WHEN Winner IS NOT NULL AND Winner != TeamNameB THEN 1 ELSE 0 END AS Loss
    FROM recent_matches
    WHERE TeamNameB IS NOT NULL
),

team_stats AS (
    SELECT
        TeamName,
        COUNT(*) AS MatchesPlayed,
        SUM(Win) AS Wins,
        SUM(Loss) AS Losses,
        ROUND(SUM(Win) * 100.0 / COUNT(*), 1) AS WinPct,
        SUM(SetsWon) AS TotalSetsWon,
        SUM(SetsLost) AS TotalSetsLost,
        MAX(DateLocal) AS LastMatchDate
    FROM team_matches
    GROUP BY TeamName
),

recent_streak AS (
    -- Calculate current streak (simplified - last 5 matches)
    SELECT
        TeamName,
        CASE
            WHEN SUM(Win) = 5 THEN '5W'
            WHEN SUM(Win) = 4 THEN '4W'
            WHEN SUM(Win) = 3 THEN '3W'
            WHEN SUM(Loss) >= 3 THEN CAST(SUM(Loss) AS VARCHAR) || 'L'
            ELSE 'Mixed'
        END AS Streak
    FROM (
        SELECT
            TeamName,
            Win,
            Loss,
            ROW_NUMBER() OVER (PARTITION BY TeamName ORDER BY DateLocal DESC) AS rn
        FROM team_matches
    )
    WHERE rn <= 5
    GROUP BY TeamName
)

SELECT
    ts.TeamName,
    ts.MatchesPlayed,
    ts.Wins,
    ts.Losses,
    ts.WinPct,
    ts.TotalSetsWon,
    ts.TotalSetsLost,
    ROUND(ts.TotalSetsWon * 1.0 / NULLIF(ts.TotalSetsLost, 0), 2) AS SetRatio,
    rs.Streak,
    ts.LastMatchDate
FROM team_stats ts
LEFT JOIN recent_streak rs
    ON ts.TeamName = rs.TeamName
WHERE ts.MatchesPlayed >= 3  -- Minimum 3 matches for meaningful stats
ORDER BY ts.WinPct DESC, ts.Wins DESC;
