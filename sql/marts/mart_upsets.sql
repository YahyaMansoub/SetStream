-- ==============================================================================
-- mart_upsets.sql  
-- Surprising match outcomes (underdog victories)
-- ==============================================================================

CREATE OR REPLACE VIEW mart_upsets AS
SELECT
    u.MatchNo,
    u.DateLocal,
    u.Winner,
    u.Loser,
    u.WinnerEloBefore,
    u.WinnerEloAfter,
    u.WinnerEloAfter - u.WinnerEloBefore AS EloGain,
    ROUND(u.ExpectedWinProb * 100, 1) AS ExpectedWinPct,
    ROUND(u.SurpriseIndex * 100, 1) AS SurpriseScore,
    m.MatchPointsA,
    m.MatchPointsB,
    m.NoTournament,
    t.Name AS TournamentName,
    t.Gender,
    m.City,
    m.CountryName AS MatchCountry,
    t.Type AS TournamentType
FROM upsets u
LEFT JOIN stg_matches m
    ON u.MatchNo = m.No
LEFT JOIN stg_tournaments t
    ON m.NoTournament = t.No
ORDER BY u.SurpriseIndex DESC, u.DateLocal DESC;
