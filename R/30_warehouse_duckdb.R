# ==============================================================================
# 30_warehouse_duckdb.R
# DuckDB warehouse operations: staging tables, upserts, mart creation
# ==============================================================================

library(DBI)
library(duckdb)
library(dplyr)
library(logger)


#' Get DuckDB connection
#'
#' @param cfg Configuration list
#' @param read_only Logical, whether connection is read-only
#' @return DBI connection
get_duckdb_conn <- function(cfg, read_only = FALSE) {
  db_path <- cfg$storage$warehouse_path
  
  # Ensure directory exists
  db_dir <- dirname(db_path)
  if (!dir.exists(db_dir)) {
    dir.create(db_dir, recursive = TRUE)
  }
  
  conn <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = db_path,
    read_only = read_only
  )
  
  logger::log_debug("DuckDB connection opened: {db_path}")
  
  conn
}


#' Close DuckDB connection safely
#'
#' @param conn DBI connection
close_duckdb_conn <- function(conn) {
  if (!is.null(conn) && DBI::dbIsValid(conn)) {
    DBI::dbDisconnect(conn, shutdown = TRUE)
    logger::log_debug("DuckDB connection closed")
  }
}


#' Create or replace staging table from data frame
#'
#' @param conn DuckDB connection
#' @param table_name Table name (e.g., "stg_tournaments")
#' @param data Data frame
#' @param primary_key Character vector of primary key columns
create_staging_table <- function(conn, table_name, data, primary_key = NULL) {
  if (is.null(data) || nrow(data) == 0) {
    logger::log_warn("No data provided for {table_name}")
    return(invisible(NULL))
  }
  
  logger::log_info("Creating staging table: {table_name}")
  
  # Drop if exists
  DBI::dbExecute(conn, glue::glue("DROP TABLE IF EXISTS {table_name}"))
  
  # Write table
  DBI::dbWriteTable(conn, table_name, data, overwrite = TRUE)
  
  # Create index on primary key if specified
  if (!is.null(primary_key) && length(primary_key) > 0) {
    idx_name <- paste0("idx_", table_name, "_pk")
    pk_cols <- paste(primary_key, collapse = ", ")
    
    tryCatch(
      {
        DBI::dbExecute(
          conn,
          glue::glue("CREATE INDEX {idx_name} ON {table_name} ({pk_cols})")
        )
        logger::log_debug("Created index on {pk_cols}")
      },
      error = function(e) {
        logger::log_warn("Failed to create index: {conditionMessage(e)}")
      }
    )
  }
  
  row_count <- DBI::dbGetQuery(conn, glue::glue("SELECT COUNT(*) as n FROM {table_name}"))$n
  logger::log_info("{table_name}: {row_count} rows")
  
  invisible(NULL)
}


#' Upsert data into staging table (insert new, update existing)
#'
#' @param conn DuckDB connection
#' @param table_name Table name
#' @param data New data frame
#' @param primary_key Primary key columns for matching
upsert_staging_table <- function(conn, table_name, data, primary_key) {
  if (is.null(data) || nrow(data) == 0) {
    logger::log_info("No new data to upsert into {table_name}")
    return(invisible(NULL))
  }
  
  # Check if table exists
  table_exists <- DBI::dbExistsTable(conn, table_name)
  
  if (!table_exists) {
    # Table doesn't exist, just create it
    logger::log_info("{table_name} does not exist, creating...")
    create_staging_table(conn, table_name, data, primary_key)
    return(invisible(NULL))
  }
  
  logger::log_info("Upserting {nrow(data)} rows into {table_name}")
  
  # Create temporary table with new data
  temp_table <- paste0(table_name, "_temp")
  DBI::dbWriteTable(conn, temp_table, data, overwrite = TRUE, temporary = TRUE)
  
  # Build merge/upsert logic
  # DuckDB supports INSERT OR REPLACE for simple upserts
  pk_cols <- paste(primary_key, collapse = ", ")
  
  # Get all columns
  all_cols <- names(data)
  
  # Delete existing rows that match primary key
  pk_conditions <- paste(
    sapply(primary_key, function(pk) {
      glue::glue("{table_name}.{pk} = {temp_table}.{pk}")
    }),
    collapse = " AND "
  )
  
  delete_sql <- glue::glue("
    DELETE FROM {table_name}
    WHERE EXISTS (
      SELECT 1 FROM {temp_table}
      WHERE {pk_conditions}
    )
  ")
  
  deleted_count <- DBI::dbExecute(conn, delete_sql)
  logger::log_debug("Deleted {deleted_count} existing rows")
  
  # Insert all rows from temp table
  insert_sql <- glue::glue("
    INSERT INTO {table_name}
    SELECT * FROM {temp_table}
  ")
  
  inserted_count <- DBI::dbExecute(conn, insert_sql)
  logger::log_info("Inserted {inserted_count} rows into {table_name}")
  
  # Drop temp table
  DBI::dbExecute(conn, glue::glue("DROP TABLE {temp_table}"))
  
  invisible(NULL)
}


#' Load all staging tables from lake data
#'
#' @param conn DuckDB connection
#' @param lake_data Named list of data frames from lake
load_staging_tables <- function(conn, lake_data) {
  logger::log_info("Loading staging tables...")
  
  # stg_tournaments
  if (!is.null(lake_data$tournaments)) {
    upsert_staging_table(
      conn,
      "stg_tournaments",
      lake_data$tournaments,
      primary_key = "No"
    )
  }
  
  # stg_matches
  if (!is.null(lake_data$matches)) {
    upsert_staging_table(
      conn,
      "stg_matches",
      lake_data$matches,
      primary_key = "No"
    )
  }
  
  # stg_match_details
  if (!is.null(lake_data$match_details)) {
    upsert_staging_table(
      conn,
      "stg_match_details",
      lake_data$match_details,
      primary_key = "No"
    )
  }
  
  # stg_tournament_rankings
  # Note: composite PK may be NoTournament + Team/Position
  # For simplicity, we'll recreate this table each time
  if (!is.null(lake_data$tournament_rankings)) {
    create_staging_table(
      conn,
      "stg_tournament_rankings",
      lake_data$tournament_rankings,
      primary_key = NULL  # Composite key handling can be complex
    )
  }
  
  logger::log_info("Staging tables loaded")
  
  invisible(NULL)
}


#' Execute SQL file to create a mart
#'
#' @param conn DuckDB connection
#' @param sql_file Path to SQL file
execute_mart_sql <- function(conn, sql_file) {
  if (!file.exists(sql_file)) {
    logger::log_warn("SQL file not found: {sql_file}")
    return(invisible(NULL))
  }
  
  logger::log_info("Executing mart SQL: {basename(sql_file)}")
  
  sql_content <- readLines(sql_file, warn = FALSE)
  sql <- paste(sql_content, collapse = "\n")
  
  tryCatch(
    {
      DBI::dbExecute(conn, sql)
      logger::log_info("Successfully created/updated mart: {basename(sql_file)}")
    },
    error = function(e) {
      logger::log_error("Failed to execute {basename(sql_file)}: {conditionMessage(e)}")
      stop(e)
    }
  )
  
  invisible(NULL)
}


#' Execute all mart SQL files
#'
#' @param conn DuckDB connection
#' @param marts_dir Directory containing SQL mart files
execute_all_marts <- function(conn, marts_dir = "sql/marts") {
  if (!dir.exists(marts_dir)) {
    logger::log_warn("Marts directory not found: {marts_dir}")
    return(invisible(NULL))
  }
  
  sql_files <- list.files(marts_dir, pattern = "\\.sql$", full.names = TRUE)
  
  if (length(sql_files) == 0) {
    logger::log_warn("No SQL files found in {marts_dir}")
    return(invisible(NULL))
  }
  
  logger::log_info("Found {length(sql_files)} mart SQL files")
  
  for (sql_file in sql_files) {
    execute_mart_sql(conn, sql_file)
  }
  
  logger::log_info("All marts executed")
  
  invisible(NULL)
}


#' Export data from DuckDB to Parquet
#'
#' @param conn DuckDB connection
#' @param table_name Table or view name
#' @param output_path Output Parquet file path
export_to_parquet <- function(conn, table_name, output_path) {
  logger::log_info("Exporting {table_name} to {output_path}")
  
  # Ensure output directory exists
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Use DuckDB's COPY command for efficient export
  sql <- glue::glue("COPY (SELECT * FROM {table_name}) TO '{output_path}' (FORMAT PARQUET)")
  
  tryCatch(
    {
      DBI::dbExecute(conn, sql)
      logger::log_info("Export complete: {output_path}")
    },
    error = function(e) {
      logger::log_error("Export failed: {conditionMessage(e)}")
      stop(e)
    }
  )
  
  invisible(NULL)
}
