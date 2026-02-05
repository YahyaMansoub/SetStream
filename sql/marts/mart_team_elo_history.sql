-- ==============================================================================
-- mart_team_elo_history.sql
-- Team Elo rating evolution over time
-- ==============================================================================

CREATE OR REPLACE VIEW mart_team_elo_history AS
SELECT
    teh.MatchNo,
    teh.DateLocal,
    teh.TeamName,
    teh.Opponent,
    teh.EloBefore,
    teh.EloAfter,
    teh.EloAfter - teh.EloBefore AS EloChange,
    teh.ExpectedScore,
    teh.ActualScore,
    teh.WinFlag,
    m.NoTournament,
    t.Name AS TournamentName,
    m.City,
    m.CountryName AS MatchCountry
FROM team_elo_history teh
LEFT JOIN stg_matches m
    ON teh.MatchNo = m.No
LEFT JOIN stg_tournaments t
    ON m.NoTournament = t.No
ORDER BY teh.DateLocal DESC, teh.TeamName;
