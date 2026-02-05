# ==============================================================================
# 50_marts_sql.R
# Helper functions for mart SQL execution
# ==============================================================================

library(DBI)
library(logger)


#' Create all marts from SQL definitions
#'
#' @param conn DuckDB connection
#' @param marts_dir Directory containing SQL files
create_marts <- function(conn, marts_dir = "sql/marts") {
  execute_all_marts(conn, marts_dir)
  
  invisible(NULL)
}


#' Get latest Elo ratings for all teams
#'
#' @param conn DuckDB connection
#' @return Data frame with current Elo ratings
get_current_elo_ratings <- function(conn) {
  if (!DBI::dbExistsTable(conn, "team_elo_history")) {
    logger::log_warn("team_elo_history table does not exist")
    return(NULL)
  }
  
  sql <- "
    SELECT
        TeamName,
        EloAfter AS CurrentElo,
        DateLocal AS LastMatchDate,
        MatchNo AS LastMatchNo
    FROM (
        SELECT
            TeamName,
            EloAfter,
            DateLocal,
            MatchNo,
            ROW_NUMBER() OVER (PARTITION BY TeamName ORDER BY DateLocal DESC, MatchNo DESC) AS rn
        FROM team_elo_history
    )
    WHERE rn = 1
    ORDER BY CurrentElo DESC
  "
  
  DBI::dbGetQuery(conn, sql)
}


#' Export key metrics to Parquet
#'
#' @param conn DuckDB connection
#' @param cfg Configuration list
export_metrics <- function(conn, cfg) {
  export_path <- file.path(cfg$storage$export_path, "latest_metrics.parquet")
  
  # Get top teams by Elo
  top_teams <- get_current_elo_ratings(conn)
  
  if (!is.null(top_teams) && nrow(top_teams) > 0) {
    export_to_parquet(conn, "team_elo_history", 
                      file.path(cfg$storage$export_path, "team_elo_history.parquet"))
    
    logger::log_info("Metrics exported successfully")
  } else {
    logger::log_warn("No metrics to export")
  }
  
  invisible(NULL)
}
