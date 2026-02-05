# ==============================================================================
# Makefile
# Automation commands for SetStream
# ==============================================================================

.PHONY: help bootstrap run api dashboard test clean test-quick

help:
	@echo "SetStream - Volleyball Analytics Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make bootstrap    - Set up environment and install dependencies"
	@echo "  make run          - Run the data pipeline"
	@echo "  make api          - Start the API server"
	@echo "  make dashboard    - Launch the Shiny dashboard"
	@echo "  make test         - Run all tests"
	@echo "  make test-quick   - Run quick tests only"
	@echo "  make clean        - Remove generated data and artifacts"
	@echo "  make clean-all    - Remove data, artifacts, and renv library"
	@echo ""

bootstrap:
	@echo "Bootstrapping SetStream environment..."
	Rscript scripts/bootstrap.R

run:
	@echo "Running SetStream pipeline..."
	Rscript scripts/run_pipeline.R

run-backfill:
	@echo "Running pipeline with backfill..."
	Rscript scripts/run_pipeline.R --backfill 730

api:
	@echo "Starting API server..."
	Rscript scripts/run_api.R

dashboard:
	@echo "Launching dashboard..."
	Rscript scripts/run_dashboard.R

test:
	@echo "Running tests..."
	Rscript -e "testthat::test_dir('tests/testthat')"

test-quick:
	@echo "Running quick tests..."
	Rscript -e "testthat::test_dir('tests/testthat', filter = 'utils|extract')"

clean:
	@echo "Cleaning generated data and artifacts..."
	-rm -rf data/lake/**/*.parquet
	-rm -f data/warehouse/*.duckdb*
	-rm -f data/state/*.json
	-rm -rf _targets/
	-rm -f logs/*.log
	@echo "Clean complete"

clean-all: clean
	@echo "Cleaning all (including renv library)..."
	-rm -rf renv/library/
	@echo "Clean all complete. Run 'make bootstrap' to restore."

# Windows-specific commands (if needed)
ifeq ($(OS),Windows_NT)
clean:
	@echo "Cleaning generated data and artifacts (Windows)..."
	-del /Q /S data\lake\*.parquet 2>nul
	-del /Q data\warehouse\*.duckdb* 2>nul
	-del /Q data\state\*.json 2>nul
	-rmdir /S /Q _targets 2>nul
	-del /Q logs\*.log 2>nul
	@echo "Clean complete"
endif
