# ==============================================================================
# run_api.R
# Start the Plumber API server
# ==============================================================================

library(plumber)
library(logger)

# Load configuration
source("R/00_utils.R")
cfg <- load_config()
setup_logging(cfg)

logger::log_info("Starting SetStream API...")

# Create and configure the API
api <- plumber::plumb("api/plumber.R")

# Get host and port from config
host <- cfg$api_service$host
port <- cfg$api_service$port

cat("\n")
cat("====================================\n")
cat("  SetStream API Server\n")
cat("====================================\n")
cat("Host:", host, "\n")
cat("Port:", port, "\n")
cat("URL:  http://localhost:", port, "\n", sep = "")
cat("====================================\n\n")

cat("Endpoints:\n")
cat("  GET  /health\n")
cat("  GET  /teams/top?limit=20\n")
cat("  GET  /teams/{team}/elo\n")
cat("  GET  /tournaments/recent\n")
cat("  GET  /upsets/recent?days=30\n\n")

cat("Press Ctrl+C to stop the server\n\n")

# Run the API
api$run(host = host, port = port)
