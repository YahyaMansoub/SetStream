# SetStream Test Suite
# Load testthat for running tests

library(testthat)
library(logger)

# Suppress log output during tests
logger::log_threshold(logger::WARN)

# Set config to testing
Sys.setenv(R_CONFIG_ACTIVE = "testing")

# Run tests
test_check("setstream")
