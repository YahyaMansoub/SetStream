# ==============================================================================
# plumber.R
# REST API endpoints for SetStream
# ==============================================================================

library(plumber)
library(DBI)
library(duckdb)
library(dplyr)
library(logger)

# Load utilities and config
source("R/00_utils.R")
cfg <- load_config()

# Get DuckDB connection (read-only)
get_conn <- function() {
  DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = cfg$storage$warehouse_path,
    read_only = TRUE
  )
}

close_conn <- function(conn) {
  if (!is.null(conn) && DBI::dbIsValid(conn)) {
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }
}

#* @apiTitle SetStream Volleyball Analytics API
#* @apiDescription REST API for volleyball data, Elo ratings, and upsets
#* @apiVersion 1.0.0

#* Health check
#* @get /health
function() {
  list(
    status = "healthy",
    service = "SetStream API",
    version = "1.0.0",
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC", tz = "UTC")
  )
}

#* Get top teams by Elo rating
#* @param limit:int Maximum number of teams to return (default 20)
#* @get /teams/top
function(limit = 20) {
  conn <- get_conn()
  on.exit(close_conn(conn))
  
  limit <- min(as.integer(limit), 100)  # Cap at 100
  
  sql <- glue::glue("
    SELECT
        TeamName,
        CurrentElo,
        LastMatchDate,
        LastMatchNo
    FROM (
        SELECT
            TeamName,
            EloAfter AS CurrentElo,
            DateLocal AS LastMatchDate,
            MatchNo AS LastMatchNo,
            ROW_NUMBER() OVER (PARTITION BY TeamName ORDER BY DateLocal DESC, MatchNo DESC) AS rn
        FROM team_elo_history
    )
    WHERE rn = 1
    ORDER BY CurrentElo DESC
    LIMIT {limit}
  ")
  
  result <- DBI::dbGetQuery(conn, sql)
  
  list(
    count = nrow(result),
    teams = result
  )
}

#* Get Elo history for a specific team
#* @param team Team name
#* @get /teams/<team>/elo
function(team) {
  conn <- get_conn()
  on.exit(close_conn(conn))
  
  sql <- "
    SELECT
        MatchNo,
        DateLocal,
        Opponent,
        EloBefore,
        EloAfter,
        EloAfter - EloBefore AS EloChange,
        WinFlag,
        TournamentName,
        MatchCountry
    FROM mart_team_elo_history
    WHERE TeamName = ?
    ORDER BY DateLocal DESC
    LIMIT 100
  "
  
  result <- DBI::dbGetQuery(conn, sql, params = list(team))
  
  if (nrow(result) == 0) {
    list(
      error = "Team not found",
      team = team
    )
  } else {
    list(
      team = team,
      count = nrow(result),
      history = result
    )
  }
}

#* Get recent tournaments
#* @param limit:int Maximum number of tournaments (default 10)
#* @get /tournaments/recent
function(limit = 10) {
  conn <- get_conn()
  on.exit(close_conn(conn))
  
  limit <- min(as.integer(limit), 50)
  
  sql <- glue::glue("
    SELECT DISTINCT
        NoTournament,
        TournamentName,
        Season,
        StartDate,
        EndDate,
        TournamentCountry,
        Gender,
        TournamentType
    FROM mart_tournament_rankings
    ORDER BY StartDate DESC
    LIMIT {limit}
  ")
  
  result <- DBI::dbGetQuery(conn, sql)
  
  list(
    count = nrow(result),
    tournaments = result
  )
}

#* Get recent upsets
#* @param days:int Number of days to look back (default 30)
#* @get /upsets/recent
function(days = 30) {
  conn <- get_conn()
  on.exit(close_conn(conn))
  
  days <- min(as.integer(days), 365)
  
  sql <- glue::glue("
    SELECT
        MatchNo,
        DateLocal,
        Winner,
        Loser,
        WinnerEloBefore,
        WinnerEloAfter,
        EloGain,
        ExpectedWinPct,
        SurpriseScore,
        TournamentName,
        MatchCountry
    FROM mart_upsets
    WHERE DateLocal >= CURRENT_DATE - INTERVAL '{days}' DAY
    ORDER BY SurpriseScore DESC
    LIMIT 50
  ")
  
  result <- DBI::dbGetQuery(conn, sql)
  
  list(
    days = days,
    count = nrow(result),
    upsets = result
  )
}
