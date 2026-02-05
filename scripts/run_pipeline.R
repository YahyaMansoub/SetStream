# ==============================================================================
# run_pipeline.R
# Execute the targets pipeline
# ==============================================================================

library(targets)

cat("\n")
cat("====================================\n")
cat("  SetStream Data Pipeline\n")
cat("====================================\n\n")

# Parse command line args for backfill option
args <- commandArgs(trailingOnly = TRUE)

if ("--backfill" %in% args) {
  idx <- which(args == "--backfill")
  if (length(args) > idx) {
    backfill_days <- as.numeric(args[idx + 1])
    cat("Backfill mode: last", backfill_days, "days\n")
    Sys.setenv(BACKFILL_DAYS = backfill_days)
  }
}

if ("--force" %in% args) {
  cat("Force refresh mode enabled\n")
  Sys.setenv(FORCE_REFRESH = "true")
}

cat("\nStarting pipeline execution...\n")
cat("(Check logs/ directory for detailed logs)\n\n")

start_time <- Sys.time()

# Run the pipeline
tryCatch(
  {
    tar_make()
    
    end_time <- Sys.time()
    elapsed <- difftime(end_time, start_time, units = "mins")
    
    cat("\n")
    cat("====================================\n")
    cat("  Pipeline Success!\n")
    cat("====================================\n")
    cat("Duration:", round(elapsed, 2), "minutes\n")
    cat("Completed at:", format(end_time, "%Y-%m-%d %H:%M:%S"), "\n\n")
    
    # Show summary
    cat("Summary:\n")
    manifest <- tar_manifest(fields = "name")
    cat("  Total targets:", nrow(manifest), "\n")
    
    meta <- tar_meta()
    if (!is.null(meta) && nrow(meta) > 0) {
      completed <- sum(meta$error == "", na.rm = TRUE)
      cat("  Completed:", completed, "\n")
    }
    
    cat("\nNext steps:\n")
    cat("  - View pipeline graph: tar_visnetwork()\n")
    cat("  - Start API: Rscript scripts/run_api.R\n")
    cat("  - Launch dashboard: Rscript scripts/run_dashboard.R\n\n")
  },
  error = function(e) {
    cat("\n")
    cat("====================================\n")
    cat("  Pipeline Failed!\n")
    cat("====================================\n")
    cat("Error:", conditionMessage(e), "\n\n")
    cat("Check logs/ directory for details\n")
    cat("Inspect failed target: tar_meta(fields = error)\n\n")
    quit(status = 1)
  }
)
