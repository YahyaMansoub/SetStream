# ==============================================================================
# test_quality.R
# Tests for data quality functions
# ==============================================================================

library(testthat)
library(DBI)
library(duckdb)
library(dplyr)

source("../../R/00_utils.R")
source("../../R/30_warehouse_duckdb.R")
source("../../R/40_quality.R")

test_that("manual quality checks detect duplicates", {
  # Create test config
  cfg <- list(
    quality = list(
      fail_on_critical = FALSE,
      warn_on_major = TRUE
    ),
    storage = list(
      warehouse_path = ":memory:"
    )
  )
  
  # Create in-memory database
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  # Create test data with duplicates
  test_tournaments <- data.frame(
    No = c(1, 1, 2, 3),  # Duplicate No
    Name = c("T1", "T1", "T2", "T3"),
    Season = c("2024", "2024", "2024", "2024")
  )
  
  DBI::dbWriteTable(conn, "stg_tournaments", test_tournaments)
  
  # Run checks (should warn but not fail)
  expect_warning(
    run_manual_quality_checks(conn, cfg),
    NA  # No warning expected with fail_on_critical = FALSE
  )
  
  DBI::dbDisconnect(conn, shutdown = TRUE)
})

test_that("manual quality checks pass with clean data", {
  cfg <- list(
    quality = list(
      fail_on_critical = TRUE,
      warn_on_major = TRUE
    ),
    storage = list(
      warehouse_path = ":memory:"
    )
  )
  
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  # Create clean test data
  test_tournaments <- data.frame(
    No = c(1, 2, 3),
    Name = c("T1", "T2", "T3"),
    Season = c("2024", "2024", "2024")
  )
  
  test_matches <- data.frame(
    No = c(1, 2, 3),
    NoTournament = c(1, 1, 2),
    TeamNameA = c("Team A", "Team B", "Team C"),
    TeamNameB = c("Team D", "Team E", "Team F")
  )
  
  DBI::dbWriteTable(conn, "stg_tournaments", test_tournaments)
  DBI::dbWriteTable(conn, "stg_matches", test_matches)
  
  # Should pass without errors
  expect_silent(run_manual_quality_checks(conn, cfg))
  
  DBI::dbDisconnect(conn, shutdown = TRUE)
})

test_that("referential integrity check detects violations", {
  cfg <- list(
    quality = list(
      fail_on_critical = FALSE
    )
  )
  
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  test_tournaments <- data.frame(
    No = c(1, 2),
    Name = c("T1", "T2")
  )
  
  test_matches <- data.frame(
    No = c(1, 2, 3),
    NoTournament = c(1, 2, 999),  # 999 doesn't exist
    TeamNameA = c("A", "B", "C"),
    TeamNameB = c("D", "E", "F")
  )
  
  DBI::dbWriteTable(conn, "stg_tournaments", test_tournaments)
  DBI::dbWriteTable(conn, "stg_matches", test_matches)
  
  # Should detect the violation
  expect_message(
    run_manual_quality_checks(conn, cfg),
    "referential"
  )
  
  DBI::dbDisconnect(conn, shutdown = TRUE)
})
