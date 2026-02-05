# ==============================================================================
# bootstrap.R
# Initialize project environment and dependencies
# ==============================================================================

cat("SetStream Bootstrap\n")
cat("===================\n\n")

# Check if R version is suitable
r_version <- getRversion()
cat("R version:", as.character(r_version), "\n")

if (r_version < "4.1.0") {
  warning("R version 4.1.0 or higher is recommended")
}

# Install renv if not present
if (!requireNamespace("renv", quietly = TRUE)) {
  cat("Installing renv...\n")
  install.packages("renv")
}

# Restore packages from lockfile
cat("\nRestoring packages from renv.lock...\n")
cat("This may take a few minutes on first run.\n\n")

renv::restore(prompt = FALSE)

# Create directory structure
cat("\nCreating directory structure...\n")

dirs <- c(
  "data/lake/tournaments",
  "data/lake/matches",
  "data/lake/match_details",
  "data/lake/tournament_rankings",
  "data/warehouse",
  "data/state",
  "logs"
)

for (dir in dirs) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
    cat("  Created:", dir, "\n")
  }
}

# Verify key packages
cat("\nVerifying key packages...\n")

key_packages <- c(
  "targets", "fivbvis", "duckdb", "arrow",
  "plumber", "shiny", "pointblank", "logger"
)

missing <- character(0)

for (pkg in key_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    missing <- c(missing, pkg)
    cat("  ✗", pkg, "NOT FOUND\n")
  } else {
    cat("  ✓", pkg, "\n")
  }
}

if (length(missing) > 0) {
  cat("\nWARNING: Some packages are missing:\n")
  cat(paste(" ", missing, collapse = "\n"), "\n")
  cat("\nTry running: renv::install(c('", paste(missing, collapse = "', '"), "'))\n", sep = "")
} else {
  cat("\n✓ All key packages available\n")
}

cat("\n===================\n")
cat("Bootstrap complete!\n\n")
cat("Next steps:\n")
cat("  1. Run pipeline:   Rscript scripts/run_pipeline.R\n")
cat("  2. Start API:      Rscript scripts/run_api.R\n")
cat("  3. Open dashboard: Rscript scripts/run_dashboard.R\n")
cat("\nOr use the Makefile:\n")
cat("  make run\n")
cat("  make api\n")
cat("  make dashboard\n")
