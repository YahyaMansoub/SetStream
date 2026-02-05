# ==============================================================================
# run_dashboard.R
# Launch the Shiny dashboard
# ==============================================================================

library(shiny)

cat("\n")
cat("====================================\n")
cat("  SetStream Dashboard\n")
cat("====================================\n\n")

cat("Launching dashboard...\n")
cat("The dashboard will open in your default browser\n")
cat("Press Ctrl+C in this terminal to stop\n\n")

# Run the Shiny app
shiny::runApp(
  "dashboard",
  port = 3838,
  launch.browser = TRUE
)
