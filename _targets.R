# ==============================================================================
# _targets.R
# Targets pipeline orchestration for SetStream
# ==============================================================================

library(targets)
library(tarchetypes)

# Source all R modules
source("R/00_utils.R")
source("R/10_extract_vis.R")
source("R/20_lake_io.R")
source("R/30_warehouse_duckdb.R")
source("R/40_quality.R")
source("R/50_marts_sql.R")
source("R/60_features_elo.R")

# Set targets options
tar_option_set(
  packages = c(
    "config", "logger", "dplyr", "tidyr", "purrr",
    "arrow", "DBI", "duckdb", "fivbvis", "pointblank",
    "lubridate", "glue", "jsonlite"
  ),
  format = "rds",
  error = "stop"
)

# Define pipeline
list(
  # ==== SETUP ====
  tar_target(
    cfg,
    {
      config <- load_config()
      setup_logging(config)
      ensure_directories(config)
      config
    }
  ),
  
  tar_target(
    state,
    load_state(cfg$storage$state_path)
  ),
  
  # ==== EXTRACT ====
  tar_target(
    tournaments_raw,
    extract_tournaments(cfg)
  ),
  
  tar_target(
    matches_raw,
    extract_matches(cfg)
  ),
  
  # Filter to rolling window
  tar_target(
    matches_filtered,
    filter_rolling_window(
      matches_raw,
      date_col = "DateLocal",
      window_days = cfg$pipeline$rolling_window_days
    )
  ),
  
  # Extract match details (incremental)
  tar_target(
    match_details_result,
    {
      if (!is.null(matches_filtered)) {
        match_nos <- matches_filtered$No
        extract_match_details_batch(match_nos, cfg, state)
      } else {
        list(data = NULL, fetched_nos = integer(0))
      }
    }
  ),
  
  # Extract tournament rankings (incremental)
  tar_target(
    tournament_rankings_result,
    {
      if (!is.null(matches_filtered)) {
        tournament_nos <- unique(matches_filtered$NoTournament)
        extract_tournament_rankings_batch(tournament_nos, cfg, state)
      } else {
        list(data = NULL, fetched_nos = integer(0))
      }
    }
  ),
  
  # Update state with newly fetched IDs
  tar_target(
    state_updated,
    {
      new_state <- update_state(
        state,
        match_nos = match_details_result$fetched_nos,
        tournament_nos = tournament_rankings_result$fetched_nos
      )
      save_state(new_state, cfg$storage$state_path)
      new_state
    }
  ),
  
  # ==== LAKE WRITES ====
  tar_target(
    lake_tournaments,
    {
      write_tournaments_to_lake(tournaments_raw, cfg)
      tournaments_raw
    }
  ),
  
  tar_target(
    lake_matches,
    {
      write_matches_to_lake(matches_filtered, cfg)
      matches_filtered
    }
  ),
  
  tar_target(
    lake_match_details,
    {
      write_match_details_to_lake(match_details_result$data, cfg)
      match_details_result$data
    }
  ),
  
  tar_target(
    lake_tournament_rankings,
    {
      write_tournament_rankings_to_lake(
        tournament_rankings_result$data,
        cfg,
        tournaments_raw
      )
      tournament_rankings_result$data
    }
  ),
  
  # ==== WAREHOUSE LOAD ====
  tar_target(
    warehouse_conn,
    get_duckdb_conn(cfg, read_only = FALSE)
  ),
  
  tar_target(
    staging_loaded,
    {
      lake_data <- list(
        tournaments = lake_tournaments,
        matches = lake_matches,
        match_details = lake_match_details,
        tournament_rankings = lake_tournament_rankings
      )
      load_staging_tables(warehouse_conn, lake_data)
      TRUE
    }
  ),
  
  # ==== QUALITY CHECKS ====
  tar_target(
    quality_results,
    {
      # Use manual checks as fallback if pointblank has issues
      tryCatch(
        run_quality_checks(warehouse_conn, cfg),
        error = function(e) {
          logger::log_warn("Pointblank checks failed, using manual checks: {conditionMessage(e)}")
          run_manual_quality_checks(warehouse_conn, cfg)
        }
      )
    }
  ),
  
  # ==== ELO COMPUTATION ====
  tar_target(
    elo_history,
    {
      matches_for_elo <- if (!is.null(lake_matches)) {
        lake_matches
      } else {
        read_from_lake(cfg$storage$lake_path, "matches")
      }
      compute_team_elo(matches_for_elo, cfg)
    }
  ),
  
  tar_target(
    upsets,
    compute_upsets(elo_history)
  ),
  
  tar_target(
    elo_written,
    {
      write_elo_to_warehouse(warehouse_conn, elo_history, upsets)
      TRUE
    }
  ),
  
  # ==== MARTS ====
  tar_target(
    marts_created,
    {
      create_marts(warehouse_conn, "sql/marts")
      TRUE
    }
  ),
  
  # ==== EXPORTS ====
  tar_target(
    metrics_exported,
    {
      export_metrics(warehouse_conn, cfg)
      TRUE
    }
  ),
  
  # ==== CLEANUP ====
  tar_target(
    pipeline_complete,
    {
      close_duckdb_conn(warehouse_conn)
      logger::log_info("✓ Pipeline complete!")
      Sys.time()
    }
  )
)
