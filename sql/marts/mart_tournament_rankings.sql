-- ==============================================================================
-- mart_tournament_rankings.sql
-- Tournament final rankings mart
-- ==============================================================================

CREATE OR REPLACE VIEW mart_tournament_rankings AS
SELECT
    tr.*,
    t.Name AS TournamentName,
    t.Season,
    t.StartDate,
    t.EndDate,
    t.CountryName AS TournamentCountry,
    t.Gender,
    t.TeamType,
    t.Type AS TournamentType
FROM stg_tournament_rankings tr
LEFT JOIN stg_tournaments t
    ON tr.NoTournament = t.No
WHERE t.No IS NOT NULL
ORDER BY t.StartDate DESC, tr.NoTournament, COALESCE(tr.Rank, tr.Position, 999);
