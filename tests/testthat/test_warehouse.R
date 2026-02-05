# ==============================================================================
# test_warehouse.R
# Tests for warehouse operations
# ==============================================================================

library(testthat)
library(DBI)
library(duckdb)
library(dplyr)

source("../../R/00_utils.R")
source("../../R/30_warehouse_duckdb.R")
source("../../R/60_features_elo.R")

test_that("DuckDB connection works", {
  cfg <- list(
    storage = list(
      warehouse_path = ":memory:"
    )
  )
  
  conn <- get_duckdb_conn(cfg, read_only = FALSE)
  
  expect_true(DBI::dbIsValid(conn))
  
  close_duckdb_conn(conn)
  
  expect_false(DBI::dbIsValid(conn))
})

test_that("create_staging_table creates table", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  test_data <- data.frame(
    No = 1:5,
    Name = paste0("Item", 1:5)
  )
  
  create_staging_table(conn, "test_table", test_data, primary_key = "No")
  
  expect_true(DBI::dbExistsTable(conn, "test_table"))
  
  result <- DBI::dbReadTable(conn, "test_table")
  expect_equal(nrow(result), 5)
  
  DBI::dbDisconnect(conn, shutdown = TRUE)
})

test_that("upsert_staging_table updates existing records", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  # Initial data
  initial_data <- data.frame(
    No = 1:3,
    Value = c(10, 20, 30)
  )
  
  create_staging_table(conn, "test_upsert", initial_data, primary_key = "No")
  
  # New data with some overlaps
  new_data <- data.frame(
    No = 2:5,
    Value = c(200, 300, 400, 500)
  )
  
  upsert_staging_table(conn, "test_upsert", new_data, primary_key = "No")
  
  result <- DBI::dbReadTable(conn, "test_upsert") %>%
    arrange(No)
  
  # Should have records 1-5
  expect_equal(nrow(result), 5)
  
  # Record 2 should be updated to 200
  expect_equal(result$Value[result$No == 2], 200)
  
  # Record 1 should remain 10
  expect_equal(result$Value[result$No == 1], 10)
  
  DBI::dbDisconnect(conn, shutdown = TRUE)
})

test_that("Elo calculation works correctly", {
  cfg <- list(
    elo = list(
      base_rating = 1500,
      k_factor = 20,
      home_advantage = 0
    )
  )
  
  # Create simple test matches
  test_matches <- data.frame(
    No = 1:3,
    DateLocal = as.Date(c("2024-01-01", "2024-01-02", "2024-01-03")),
    TeamNameA = c("Team A", "Team B", "Team A"),
    TeamNameB = c("Team B", "Team C", "Team C"),
    MatchPointsA = c(3, 3, 1),
    MatchPointsB = c(1, 2, 3)
  )
  
  elo_history <- compute_team_elo(test_matches, cfg)
  
  # Basic checks
  expect_s3_class(elo_history, "data.frame")
  expect_gt(nrow(elo_history), 0)
  expect_true("EloAfter" %in% names(elo_history))
  
  # Winning teams should gain Elo
  team_a_match_1 <- elo_history %>%
    filter(TeamName == "Team A", MatchNo == 1)
  
  expect_gt(team_a_match_1$EloAfter, team_a_match_1$EloBefore)
})

test_that("compute_upsets identifies underdog victories", {
  # Create Elo history with an upset
  elo_history <- data.frame(
    MatchNo = c(1, 1),
    DateLocal = as.Date(c("2024-01-01", "2024-01-01")),
    TeamName = c("Weak Team", "Strong Team"),
    Opponent = c("Strong Team", "Weak Team"),
    EloBefore = c(1400, 1600),
    EloAfter = c(1420, 1580),
    ExpectedScore = c(0.24, 0.76),
    ActualScore = c(1, 0),
    WinFlag = c(TRUE, FALSE)
  )
  
  upsets <- compute_upsets(elo_history)
  
  expect_s3_class(upsets, "data.frame")
  expect_gt(nrow(upsets), 0)
  
  # Should identify Weak Team's win as an upset
  expect_true("Weak Team" %in% upsets$Winner)
  expect_true(upsets$SurpriseIndex[upsets$Winner == "Weak Team"] > 0.5)
})
