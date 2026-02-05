# ==============================================================================
# 20_lake_io.R
# Data lake I/O operations with Parquet partitioning
# ==============================================================================

library(arrow)
library(dplyr)
library(logger)


#' Write data to partitioned Parquet in the lake
#'
#' @param data Data frame to write
#' @param lake_path Base lake path
#' @param entity Entity name (tournaments, matches, match_details, tournament_rankings)
#' @param partition_cols Character vector of partition column names
write_to_lake <- function(data, lake_path, entity, partition_cols = NULL) {
  if (is.null(data) || nrow(data) == 0) {
    logger::log_info("No data to write for {entity}")
    return(invisible(NULL))
  }
  
  entity_path <- file.path(lake_path, entity)
  
  logger::log_info("Writing {nrow(data)} rows to lake: {entity}")
  
  # Ensure directory exists
  if (!dir.exists(entity_path)) {
    dir.create(entity_path, recursive = TRUE)
  }
  
  tryCatch(
    {
      if (!is.null(partition_cols) && length(partition_cols) > 0) {
        # Partitioned write
        arrow::write_dataset(
          data,
          path = entity_path,
          format = "parquet",
          partitioning = partition_cols,
          existing_data_behavior = "overwrite"
        )
        logger::log_info("Wrote partitioned by: {paste(partition_cols, collapse = ', ')}")
      } else {
        # Single file write
        output_file <- file.path(entity_path, paste0(entity, ".parquet"))
        arrow::write_parquet(data, output_file)
        logger::log_info("Wrote to: {output_file}")
      }
    },
    error = function(e) {
      logger::log_error("Failed to write {entity} to lake: {conditionMessage(e)}")
      stop(e)
    }
  )
  
  invisible(data)
}


#' Read data from partitioned Parquet in the lake
#'
#' @param lake_path Base lake path
#' @param entity Entity name
#' @return Data frame or NULL if not found
read_from_lake <- function(lake_path, entity) {
  entity_path <- file.path(lake_path, entity)
  
  if (!dir.exists(entity_path)) {
    logger::log_warn("Lake path does not exist: {entity_path}")
    return(NULL)
  }
  
  logger::log_info("Reading from lake: {entity}")
  
  data <- tryCatch(
    {
      # Try reading as dataset (handles both partitioned and single files)
      ds <- arrow::open_dataset(entity_path, format = "parquet")
      df <- dplyr::collect(ds)
      log_data_summary(df, entity)
      df
    },
    error = function(e) {
      logger::log_error("Failed to read {entity} from lake: {conditionMessage(e)}")
      NULL
    }
  )
  
  data
}


#' Write tournaments to lake with Season partitioning
#'
#' @param tournaments Data frame of tournaments
#' @param cfg Configuration list
write_tournaments_to_lake <- function(tournaments, cfg) {
  # Add partition column if not present
  if (!"Season" %in% names(tournaments) || all(is.na(tournaments$Season))) {
    # Derive season from StartDate if possible
    if ("StartDate" %in% names(tournaments)) {
      tournaments <- tournaments %>%
        dplyr::mutate(
          Season = dplyr::if_else(
            !is.na(StartDate),
            as.character(lubridate::year(StartDate)),
            "unknown"
          )
        )
    } else {
      tournaments$Season <- "unknown"
    }
  }
  
  write_to_lake(
    data = tournaments,
    lake_path = cfg$storage$lake_path,
    entity = "tournaments",
    partition_cols = "Season"
  )
}


#' Write matches to lake with year(DateLocal) partitioning
#'
#' @param matches Data frame of matches
#' @param cfg Configuration list
write_matches_to_lake <- function(matches, cfg) {
  # Add year partition column
  if ("DateLocal" %in% names(matches)) {
    matches <- matches %>%
      dplyr::mutate(
        year = dplyr::if_else(
          !is.na(DateLocal),
          as.character(lubridate::year(DateLocal)),
          "unknown"
        )
      )
  } else {
    matches$year <- "unknown"
  }
  
  write_to_lake(
    data = matches,
    lake_path = cfg$storage$lake_path,
    entity = "matches",
    partition_cols = "year"
  )
}


#' Write match details to lake with year partitioning
#'
#' @param match_details Data frame of match details
#' @param cfg Configuration list
write_match_details_to_lake <- function(match_details, cfg) {
  # Attempt to add year partition if DateLocal exists
  # Note: match_details may not have DateLocal; we might need to join with matches
  if ("DateLocal" %in% names(match_details)) {
    match_details <- match_details %>%
      dplyr::mutate(
        year = dplyr::if_else(
          !is.na(DateLocal),
          as.character(lubridate::year(DateLocal)),
          "unknown"
        )
      )
    partition_cols <- "year"
  } else {
    # No partitioning if DateLocal not available
    partition_cols <- NULL
  }
  
  write_to_lake(
    data = match_details,
    lake_path = cfg$storage$lake_path,
    entity = "match_details",
    partition_cols = partition_cols
  )
}


#' Write tournament rankings to lake with Season partitioning
#'
#' @param tournament_rankings Data frame of tournament rankings
#' @param cfg Configuration list
#' @param tournaments Data frame of tournaments (for join to get Season)
write_tournament_rankings_to_lake <- function(tournament_rankings, cfg, tournaments = NULL) {
  # Join with tournaments to get Season if available
  if (!is.null(tournaments) && "NoTournament" %in% names(tournament_rankings)) {
    tournament_rankings <- tournament_rankings %>%
      dplyr::left_join(
        tournaments %>% dplyr::select(No, Season),
        by = c("NoTournament" = "No")
      )
  }
  
  # Default to "unknown" if Season not available
  if (!"Season" %in% names(tournament_rankings)) {
    tournament_rankings$Season <- "unknown"
  }
  
  write_to_lake(
    data = tournament_rankings,
    lake_path = cfg$storage$lake_path,
    entity = "tournament_rankings",
    partition_cols = "Season"
  )
}


#' Read all entities from lake
#'
#' @param cfg Configuration list
#' @return Named list of data frames
read_all_from_lake <- function(cfg) {
  list(
    tournaments = read_from_lake(cfg$storage$lake_path, "tournaments"),
    matches = read_from_lake(cfg$storage$lake_path, "matches"),
    match_details = read_from_lake(cfg$storage$lake_path, "match_details"),
    tournament_rankings = read_from_lake(cfg$storage$lake_path, "tournament_rankings")
  )
}
