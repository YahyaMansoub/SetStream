# ==============================================================================
# 40_quality.R
# Data quality checks using pointblank
# ==============================================================================

library(pointblank)
library(dplyr)
library(logger)


#' Run data quality checks on staging tables
#'
#' @param conn DuckDB connection
#' @param cfg Configuration list
#' @return Validation agent with results
run_quality_checks <- function(conn, cfg) {
  logger::log_info("Running data quality checks...")
  
  # Create validation agent
  agent <- pointblank::create_agent(
    tbl = NULL,  # We'll set tables per check
    label = "SetStream Data Quality",
    actions = pointblank::action_levels(
      warn_at = 0.05,
      stop_at = 0.10
    )
  )
  
  # Check 1: stg_tournaments - No is unique and not null
  if (DBI::dbExistsTable(conn, "stg_tournaments")) {
    tournaments_tbl <- dplyr::tbl(conn, "stg_tournaments")
    
    agent <- agent %>%
      pointblank::col_vals_not_null(
        columns = "No",
        label = "tournaments_no_not_null",
        tbl = tournaments_tbl
      ) %>%
      pointblank::col_is_unique(
        columns = "No",
        label = "tournaments_no_unique",
        tbl = tournaments_tbl
      )
  }
  
  # Check 2: stg_matches - No is unique, required fields not null
  if (DBI::dbExistsTable(conn, "stg_matches")) {
    matches_tbl <- dplyr::tbl(conn, "stg_matches")
    
    agent <- agent %>%
      pointblank::col_vals_not_null(
        columns = "No",
        label = "matches_no_not_null",
        tbl = matches_tbl
      ) %>%
      pointblank::col_is_unique(
        columns = "No",
        label = "matches_no_unique",
        tbl = matches_tbl
      ) %>%
      pointblank::col_vals_not_null(
        columns = c("TeamNameA", "TeamNameB"),
        label = "matches_teams_not_null",
        tbl = matches_tbl
      )
    
    # Check referential integrity: NoTournament exists in tournaments
    if (DBI::dbExistsTable(conn, "stg_tournaments")) {
      # This requires a join - we'll check with a custom SQL validation
      ref_check_sql <- "
        SELECT COUNT(*) as invalid_count
        FROM stg_matches m
        LEFT JOIN stg_tournaments t ON m.NoTournament = t.No
        WHERE t.No IS NULL
      "
      
      ref_result <- DBI::dbGetQuery(conn, ref_check_sql)
      
      if (ref_result$invalid_count > 0) {
        logger::log_warn(
          "Referential integrity violation: {ref_result$invalid_count} matches ",
          "reference non-existent tournaments"
        )
        
        if (cfg$quality$fail_on_critical) {
          stop("Critical referential integrity failure in matches")
        }
      } else {
        logger::log_info("✓ Referential integrity check passed: matches -> tournaments")
      }
    }
  }
  
  # Check 3: stg_match_details - No is unique
  if (DBI::dbExistsTable(conn, "stg_match_details")) {
    match_details_tbl <- dplyr::tbl(conn, "stg_match_details")
    
    agent <- agent %>%
      pointblank::col_vals_not_null(
        columns = "No",
        label = "match_details_no_not_null",
        tbl = match_details_tbl
      ) %>%
      pointblank::col_is_unique(
        columns = "No",
        label = "match_details_no_unique",
        tbl = match_details_tbl
      )
  }
  
  # Check 4: Numeric scores are non-negative
  if (DBI::dbExistsTable(conn, "stg_matches")) {
    matches_tbl <- dplyr::tbl(conn, "stg_matches")
    
    if ("MatchPointsA" %in% DBI::dbListFields(conn, "stg_matches")) {
      agent <- agent %>%
        pointblank::col_vals_gte(
          columns = "MatchPointsA",
          value = 0,
          label = "matches_score_a_non_negative",
          tbl = matches_tbl,
          na_pass = TRUE
        )
    }
    
    if ("MatchPointsB" %in% DBI::dbListFields(conn, "stg_matches")) {
      agent <- agent %>%
        pointblank::col_vals_gte(
          columns = "MatchPointsB",
          value = 0,
          label = "matches_score_b_non_negative",
          tbl = matches_tbl,
          na_pass = TRUE
        )
    }
  }
  
  # Execute all checks
  agent <- pointblank::interrogate(agent)
  
  # Get summary
  report <- pointblank::get_agent_report(agent, display_table = FALSE)
  
  # Count failures
  failed_checks <- sum(report$f_failed > 0, na.rm = TRUE)
  warned_checks <- sum(report$f_warned > 0, na.rm = TRUE)
  
  logger::log_info(
    "Quality checks complete: {nrow(report)} checks, ",
    "{failed_checks} failures, {warned_checks} warnings"
  )
  
  # Stop if critical failures
  if (failed_checks > 0 && cfg$quality$fail_on_critical) {
    logger::log_error("Critical data quality failures detected!")
    print(report[report$f_failed > 0, ])
    stop("Pipeline stopped due to data quality failures")
  }
  
  agent
}


#' Simple manual quality checks (alternative to pointblank)
#'
#' @param conn DuckDB connection
#' @param cfg Configuration list
run_manual_quality_checks <- function(conn, cfg) {
  logger::log_info("Running manual quality checks...")
  
  failures <- list()
  
  # Check: tournaments.No is unique
  if (DBI::dbExistsTable(conn, "stg_tournaments")) {
    dup_check <- DBI::dbGetQuery(conn, "
      SELECT No, COUNT(*) as cnt
      FROM stg_tournaments
      GROUP BY No
      HAVING COUNT(*) > 1
    ")
    
    if (nrow(dup_check) > 0) {
      msg <- "Duplicate tournament No found: {nrow(dup_check)} duplicates"
      logger::log_error(msg)
      failures <- c(failures, msg)
    } else {
      logger::log_info("✓ tournaments.No is unique")
    }
  }
  
  # Check: matches.No is unique
  if (DBI::dbExistsTable(conn, "stg_matches")) {
    dup_check <- DBI::dbGetQuery(conn, "
      SELECT No, COUNT(*) as cnt
      FROM stg_matches
      GROUP BY No
      HAVING COUNT(*) > 1
    ")
    
    if (nrow(dup_check) > 0) {
      msg <- "Duplicate match No found: {nrow(dup_check)} duplicates"
      logger::log_error(msg)
      failures <- c(failures, msg)
    } else {
      logger::log_info("✓ matches.No is unique")
    }
  }
  
  # Check: matches reference valid tournaments
  if (DBI::dbExistsTable(conn, "stg_matches") && DBI::dbExistsTable(conn, "stg_tournaments")) {
    ref_check <- DBI::dbGetQuery(conn, "
      SELECT COUNT(*) as invalid_count
      FROM stg_matches m
      LEFT JOIN stg_tournaments t ON m.NoTournament = t.No
      WHERE t.No IS NULL
    ")
    
    if (ref_check$invalid_count > 0) {
      msg <- "Referential integrity: {ref_check$invalid_count} matches reference invalid tournaments"
      logger::log_error(msg)
      failures <- c(failures, msg)
    } else {
      logger::log_info("✓ Referential integrity: matches -> tournaments")
    }
  }
  
  # Check: teams not null
  if (DBI::dbExistsTable(conn, "stg_matches")) {
    null_check <- DBI::dbGetQuery(conn, "
      SELECT COUNT(*) as null_count
      FROM stg_matches
      WHERE TeamNameA IS NULL OR TeamNameB IS NULL
    ")
    
    if (null_check$null_count > 0) {
      msg <- "Missing team names: {null_check$null_count} matches"
      logger::log_warn(msg)
    } else {
      logger::log_info("✓ Team names are complete")
    }
  }
  
  # Fail if critical issues
  if (length(failures) > 0 && cfg$quality$fail_on_critical) {
    stop("Critical quality failures: ", paste(failures, collapse = "; "))
  }
  
  logger::log_info("Manual quality checks complete")
  
  invisible(NULL)
}
