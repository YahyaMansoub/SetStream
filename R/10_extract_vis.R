# ==============================================================================
# 10_extract_vis.R
# Data extraction from FIVB VIS API using fivbvis package
# ==============================================================================

library(fivbvis)
library(dplyr)
library(logger)


#' Extract tournament list with rate limiting and retries
#'
#' @param cfg Configuration list
#' @param fields Fields to select (NULL = all)
#' @return Data frame of tournaments
extract_tournaments <- function(cfg, fields = NULL) {
  logger::log_info("Extracting tournament list...")
  
  # Use configured fields if not specified
  if (is.null(fields)) {
    fields <- cfg$fields$tournament
  }
  
  tournaments <- with_retry(
    expr = {
      with_rate_limit(
        expr = {
          fivbvis::v_get_volley_tournament_list(fields = fields)
        },
        delay = cfg$api$rate_limit_delay
      )
    },
    max_retries = cfg$api$max_retries,
    backoff_base = cfg$api$retry_backoff_base
  )
  
  # Basic cleaning
  if (!is.null(tournaments) && nrow(tournaments) > 0) {
    # Parse dates if present
    if ("StartDate" %in% names(tournaments)) {
      tournaments$StartDate <- safe_parse_date(tournaments$StartDate)
    }
    if ("EndDate" %in% names(tournaments)) {
      tournaments$EndDate <- safe_parse_date(tournaments$EndDate)
    }
    
    # Ensure No is integer
    if ("No" %in% names(tournaments)) {
      tournaments$No <- as.integer(tournaments$No)
    }
    
    log_data_summary(tournaments, "tournaments")
  } else {
    logger::log_warn("No tournaments extracted!")
  }
  
  tournaments
}


#' Extract match list with rate limiting and retries
#'
#' @param cfg Configuration list
#' @param fields Fields to select (NULL = all)
#' @return Data frame of matches
extract_matches <- function(cfg, fields = NULL) {
  logger::log_info("Extracting match list...")
  
  # Use configured fields if not specified
  if (is.null(fields)) {
    fields <- cfg$fields$match
  }
  
  matches <- with_retry(
    expr = {
      with_rate_limit(
        expr = {
          fivbvis::v_get_volley_match_list(fields = fields)
        },
        delay = cfg$api$rate_limit_delay
      )
    },
    max_retries = cfg$api$max_retries,
    backoff_base = cfg$api$retry_backoff_base
  )
  
  # Basic cleaning
  if (!is.null(matches) && nrow(matches) > 0) {
    # Parse date
    if ("DateLocal" %in% names(matches)) {
      matches$DateLocal <- safe_parse_date(matches$DateLocal)
    }
    
    # Ensure IDs are integer
    if ("No" %in% names(matches)) {
      matches$No <- as.integer(matches$No)
    }
    if ("NoTournament" %in% names(matches)) {
      matches$NoTournament <- as.integer(matches$NoTournament)
    }
    
    # Ensure scores are numeric
    if ("MatchPointsA" %in% names(matches)) {
      matches$MatchPointsA <- as.numeric(matches$MatchPointsA)
    }
    if ("MatchPointsB" %in% names(matches)) {
      matches$MatchPointsB <- as.numeric(matches$MatchPointsB)
    }
    
    log_data_summary(matches, "matches")
  } else {
    logger::log_warn("No matches extracted!")
  }
  
  matches
}


#' Extract detailed match data for a single match
#'
#' @param match_no Match number
#' @param cfg Configuration list
#' @return Data frame with match details (may be multi-row for set-level data)
extract_match_detail <- function(match_no, cfg) {
  logger::log_debug("Extracting match detail: No={match_no}")
  
  match_detail <- with_retry(
    expr = {
      with_rate_limit(
        expr = {
          fivbvis::v_get_volley_match(match_no)
        },
        delay = cfg$api$rate_limit_delay
      )
    },
    max_retries = cfg$api$max_retries,
    backoff_base = cfg$api$retry_backoff_base,
    on_error = function(e, attempt) {
      logger::log_warn("Match {match_no} detail fetch failed (attempt {attempt}): {conditionMessage(e)}")
    }
  )
  
  # Add match_no as primary key if not present
  if (!is.null(match_detail) && nrow(match_detail) > 0 && !"No" %in% names(match_detail)) {
    match_detail$No <- match_no
  }
  
  match_detail
}


#' Extract match details in batch (incremental)
#'
#' @param match_nos Vector of match numbers to fetch
#' @param cfg Configuration list
#' @param state Current state (to track already-fetched matches)
#' @return List with data frame of all match details and updated match_nos
extract_match_details_batch <- function(match_nos, cfg, state) {
  # Filter out already-fetched matches
  new_match_nos <- setdiff(match_nos, state$fetched_match_nos)
  
  if (length(new_match_nos) == 0) {
    logger::log_info("No new matches to fetch details for")
    return(list(data = NULL, fetched_nos = integer(0)))
  }
  
  logger::log_info("Fetching details for {length(new_match_nos)} new matches...")
  
  all_details <- list()
  fetched_nos <- integer(0)
  
  for (i in seq_along(new_match_nos)) {
    match_no <- new_match_nos[i]
    
    if (i %% 10 == 0) {
      logger::log_info("Progress: {i}/{length(new_match_nos)} match details fetched")
    }
    
    details <- tryCatch(
      {
        extract_match_detail(match_no, cfg)
      },
      error = function(e) {
        logger::log_error("Failed to fetch match {match_no}: {conditionMessage(e)}")
        NULL
      }
    )
    
    if (!is.null(details) && nrow(details) > 0) {
      all_details[[length(all_details) + 1]] <- details
      fetched_nos <- c(fetched_nos, match_no)
    }
  }
  
  # Combine all details
  combined_details <- if (length(all_details) > 0) {
    dplyr::bind_rows(all_details)
  } else {
    NULL
  }
  
  log_data_summary(combined_details, "match_details")
  
  list(data = combined_details, fetched_nos = fetched_nos)
}


#' Extract tournament ranking for a single tournament
#'
#' @param tournament_no Tournament number
#' @param cfg Configuration list
#' @return Data frame with tournament rankings
extract_tournament_ranking <- function(tournament_no, cfg) {
  logger::log_debug("Extracting tournament ranking: No={tournament_no}")
  
  ranking <- with_retry(
    expr = {
      with_rate_limit(
        expr = {
          fivbvis::v_get_volley_tournament_ranking(tournament_no)
        },
        delay = cfg$api$rate_limit_delay
      )
    },
    max_retries = cfg$api$max_retries,
    backoff_base = cfg$api$retry_backoff_base,
    on_error = function(e, attempt) {
      logger::log_warn("Tournament {tournament_no} ranking fetch failed (attempt {attempt}): {conditionMessage(e)}")
    }
  )
  
  # Add tournament_no as foreign key if not present
  if (!is.null(ranking) && nrow(ranking) > 0 && !"NoTournament" %in% names(ranking)) {
    ranking$NoTournament <- tournament_no
  }
  
  ranking
}


#' Extract tournament rankings in batch (incremental)
#'
#' @param tournament_nos Vector of tournament numbers to fetch
#' @param cfg Configuration list
#' @param state Current state (to track already-fetched tournaments)
#' @return List with data frame of all rankings and updated tournament_nos
extract_tournament_rankings_batch <- function(tournament_nos, cfg, state) {
  # Filter out already-fetched tournaments
  new_tournament_nos <- setdiff(tournament_nos, state$fetched_tournament_nos)
  
  if (length(new_tournament_nos) == 0) {
    logger::log_info("No new tournament rankings to fetch")
    return(list(data = NULL, fetched_nos = integer(0)))
  }
  
  logger::log_info("Fetching rankings for {length(new_tournament_nos)} new tournaments...")
  
  all_rankings <- list()
  fetched_nos <- integer(0)
  
  for (i in seq_along(new_tournament_nos)) {
    tournament_no <- new_tournament_nos[i]
    
    if (i %% 5 == 0) {
      logger::log_info("Progress: {i}/{length(new_tournament_nos)} tournament rankings fetched")
    }
    
    ranking <- tryCatch(
      {
        extract_tournament_ranking(tournament_no, cfg)
      },
      error = function(e) {
        logger::log_error("Failed to fetch ranking for tournament {tournament_no}: {conditionMessage(e)}")
        NULL
      }
    )
    
    if (!is.null(ranking) && nrow(ranking) > 0) {
      all_rankings[[length(all_rankings) + 1]] <- ranking
      fetched_nos <- c(fetched_nos, tournament_no)
    }
  }
  
  # Combine all rankings
  combined_rankings <- if (length(all_rankings) > 0) {
    dplyr::bind_rows(all_rankings)
  } else {
    NULL
  }
  
  log_data_summary(combined_rankings, "tournament_rankings")
  
  list(data = combined_rankings, fetched_nos = fetched_nos)
}


#' Filter data to rolling window
#'
#' @param data Data frame with a date column
#' @param date_col Name of date column
#' @param window_days Number of days in rolling window
#' @return Filtered data frame
filter_rolling_window <- function(data, date_col = "DateLocal", window_days) {
  if (is.null(data) || nrow(data) == 0) {
    return(data)
  }
  
  cutoff_date <- Sys.Date() - window_days
  
  before_count <- nrow(data)
  data_filtered <- data %>%
    dplyr::filter(!!rlang::sym(date_col) >= cutoff_date)
  after_count <- nrow(data_filtered)
  
  logger::log_info(
    "Rolling window filter ({window_days} days): ",
    "{before_count} -> {after_count} rows ",
    "(removed {before_count - after_count})"
  )
  
  data_filtered
}
