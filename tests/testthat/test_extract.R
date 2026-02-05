# ==============================================================================
# test_extract.R
# Tests for data extraction functions
# ==============================================================================

library(testthat)
library(dplyr)

# Source the extraction module
source("../../R/00_utils.R")
source("../../R/10_extract_vis.R")

test_that("filter_rolling_window filters correctly", {
  # Create test data
  test_data <- data.frame(
    No = 1:10,
    DateLocal = seq(Sys.Date() - 400, Sys.Date() - 10, length.out = 10),
    TeamNameA = rep("Team A", 10),
    TeamNameB = rep("Team B", 10)
  )
  
  # Filter to 365 days
  filtered <- filter_rolling_window(test_data, "DateLocal", 365)
  
  # Should remove some old rows
  expect_lt(nrow(filtered), nrow(test_data))
  
  # All remaining dates should be within window
  cutoff <- Sys.Date() - 365
  expect_true(all(filtered$DateLocal >= cutoff))
})

test_that("filter_rolling_window handles empty data", {
  empty_data <- data.frame(
    No = integer(0),
    DateLocal = as.Date(character(0))
  )
  
  result <- filter_rolling_window(empty_data, "DateLocal", 365)
  
  expect_equal(nrow(result), 0)
})

test_that("safe_parse_date handles valid dates", {
  date_str <- "2024-01-15"
  result <- safe_parse_date(date_str)
  
  expect_s3_class(result, "Date")
  expect_equal(as.character(result), date_str)
})

test_that("safe_parse_date handles invalid dates", {
  date_str <- "not-a-date"
  result <- safe_parse_date(date_str)
  
  expect_true(is.na(result))
})

test_that("with_retry succeeds on first attempt", {
  counter <- 0
  
  result <- with_retry(
    expr = {
      counter <- counter + 1
      "success"
    },
    max_retries = 3
  )
  
  expect_equal(result, "success")
})

test_that("with_retry retries on failure", {
  attempt_count <- 0
  
  result <- with_retry(
    expr = {
      attempt_count <<- attempt_count + 1
      if (attempt_count < 3) {
        stop("Simulated failure")
      }
      "success"
    },
    max_retries = 3,
    backoff_base = 0.01  # Fast for testing
  )
  
  expect_equal(result, "success")
  expect_equal(attempt_count, 3)
})
