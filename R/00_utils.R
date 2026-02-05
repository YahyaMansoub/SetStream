# ==============================================================================
# 00_utils.R
# Core utilities: logging, config, retries, state management
# ==============================================================================

#' Load configuration
#'
#' @param env Environment name (default, testing, production)
#' @return List of configuration values
load_config <- function(env = Sys.getenv("R_CONFIG_ACTIVE", "default")) {
  config::get(config = env, file = "config.yml")
}


#' Initialize logging
#'
#' @param cfg Configuration list
setup_logging <- function(cfg) {
  log_level <- cfg$logging$level
  log_file <- cfg$logging$file
  
  # Create log directory if needed
  log_dir <- dirname(log_file)
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE)
  }
  
  # Set log level
  logger::log_threshold(log_level)
  
  # Add file appender if specified
  if (!is.null(log_file)) {
    logger::log_appender(
      logger::appender_tee(log_file),
      index = 1
    )
  }
  
  # Log layout with timestamp
  logger::log_layout(logger::layout_glue_colors)
  
  logger::log_info("Logging initialized: level={log_level}, file={log_file}")
}


#' Execute with retry logic
#'
#' @param expr Expression to execute
#' @param max_retries Maximum number of retry attempts
#' @param backoff_base Base for exponential backoff (seconds)
#' @param on_error Optional callback function(error, attempt)
#' @return Result of expression or error
with_retry <- function(expr, 
                       max_retries = 3, 
                       backoff_base = 2,
                       on_error = NULL) {
  
  attempt <- 1
  last_error <- NULL
  
  while (attempt <= max_retries) {
    result <- tryCatch(
      {
        return(expr)
      },
      error = function(e) {
        last_error <<- e
        
        # Log the error
        logger::log_warn(
          "Attempt {attempt}/{max_retries} failed: {conditionMessage(e)}"
        )
        
        # Call error callback if provided
        if (!is.null(on_error)) {
          on_error(e, attempt)
        }
        
        # Calculate backoff delay
        if (attempt < max_retries) {
          delay <- backoff_base ^ (attempt - 1)
          logger::log_debug("Retrying in {delay} seconds...")
          Sys.sleep(delay)
        }
        
        NULL
      }
    )
    
    attempt <- attempt + 1
  }
  
  # If we've exhausted retries, throw the last error
  logger::log_error(
    "Max retries ({max_retries}) exceeded. Last error: {conditionMessage(last_error)}"
  )
  stop(last_error)
}


#' Rate-limited execution wrapper
#'
#' @param expr Expression to execute
#' @param delay Minimum delay between calls (seconds)
#' @return Result of expression
with_rate_limit <- function(expr, delay = 1.0) {
  # Simple rate limiting using environment variable to track last call time
  last_call_env <- "SETSTREAM_LAST_API_CALL"
  
  last_call <- Sys.getenv(last_call_env, unset = "0")
  last_call_time <- as.numeric(last_call)
  current_time <- as.numeric(Sys.time())
  
  elapsed <- current_time - last_call_time
  
  if (elapsed < delay) {
    wait_time <- delay - elapsed
    logger::log_trace("Rate limiting: waiting {round(wait_time, 2)}s")
    Sys.sleep(wait_time)
  }
  
  result <- expr
  
  # Update last call time
  Sys.setenv(SETSTREAM_LAST_API_CALL = as.character(as.numeric(Sys.time())))
  
  result
}


#' Load pipeline state from disk
#'
#' @param state_path Path to state file (JSON)
#' @return List with state info (last_run, fetched_match_nos, fetched_tournament_nos)
load_state <- function(state_path) {
  if (!file.exists(state_path)) {
    logger::log_info("No existing state file. Starting fresh.")
    return(list(
      last_run = NULL,
      fetched_match_nos = integer(0),
      fetched_tournament_nos = integer(0),
      created_at = Sys.time()
    ))
  }
  
  state <- jsonlite::fromJSON(state_path, simplifyVector = FALSE)
  
  # Convert to proper types
  state$last_run <- if (!is.null(state$last_run)) {
    as.POSIXct(state$last_run, tz = "UTC")
  } else {
    NULL
  }
  
  state$fetched_match_nos <- unlist(state$fetched_match_nos)
  state$fetched_tournament_nos <- unlist(state$fetched_tournament_nos)
  
  logger::log_info(
    "State loaded: last_run={state$last_run}, ",
    "{length(state$fetched_match_nos)} matches, ",
    "{length(state$fetched_tournament_nos)} tournaments"
  )
  
  state
}


#' Save pipeline state to disk
#'
#' @param state State list
#' @param state_path Path to state file (JSON)
save_state <- function(state, state_path) {
  # Ensure directory exists
  state_dir <- dirname(state_path)
  if (!dir.exists(state_dir)) {
    dir.create(state_dir, recursive = TRUE)
  }
  
  # Update last_run
  state$last_run <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  
  # Write JSON
  jsonlite::write_json(
    state,
    state_path,
    pretty = TRUE,
    auto_unbox = TRUE
  )
  
  logger::log_info("State saved to {state_path}")
  
  invisible(state)
}


#' Update state with new fetched IDs
#'
#' @param state Current state list
#' @param match_nos New match numbers fetched
#' @param tournament_nos New tournament numbers fetched
#' @return Updated state list
update_state <- function(state, match_nos = NULL, tournament_nos = NULL) {
  if (!is.null(match_nos) && length(match_nos) > 0) {
    state$fetched_match_nos <- unique(c(state$fetched_match_nos, match_nos))
    logger::log_debug("Added {length(match_nos)} match IDs to state")
  }
  
  if (!is.null(tournament_nos) && length(tournament_nos) > 0) {
    state$fetched_tournament_nos <- unique(c(
      state$fetched_tournament_nos, 
      tournament_nos
    ))
    logger::log_debug("Added {length(tournament_nos)} tournament IDs to state")
  }
  
  state
}


#' Ensure required directories exist
#'
#' @param cfg Configuration list
ensure_directories <- function(cfg) {
  dirs <- c(
    cfg$storage$lake_path,
    file.path(cfg$storage$lake_path, "tournaments"),
    file.path(cfg$storage$lake_path, "matches"),
    file.path(cfg$storage$lake_path, "match_details"),
    file.path(cfg$storage$lake_path, "tournament_rankings"),
    dirname(cfg$storage$warehouse_path),
    dirname(cfg$storage$state_path),
    cfg$storage$export_path,
    dirname(cfg$logging$file)
  )
  
  for (d in dirs) {
    if (!dir.exists(d)) {
      dir.create(d, recursive = TRUE)
      logger::log_debug("Created directory: {d}")
    }
  }
  
  invisible(TRUE)
}


#' Parse date safely
#'
#' @param date_str Date string
#' @param format Expected format
#' @return Date object or NA
safe_parse_date <- function(date_str, format = "%Y-%m-%d") {
  tryCatch(
    as.Date(date_str, format = format),
    error = function(e) {
      logger::log_warn("Failed to parse date: {date_str}")
      NA
    }
  )
}


#' Check if we're in test mode
#'
#' @return TRUE if testing environment is active
is_testing <- function() {
  identical(Sys.getenv("R_CONFIG_ACTIVE"), "testing") ||
    identical(Sys.getenv("TESTTHAT"), "true")
}


#' Create a summary table for logging/display
#'
#' @param data Data frame
#' @param name Dataset name
#' @return Invisible NULL (logs summary)
log_data_summary <- function(data, name = "data") {
  if (is.null(data) || nrow(data) == 0) {
    logger::log_info("{name}: 0 rows")
    return(invisible(NULL))
  }
  
  logger::log_info(
    "{name}: {nrow(data)} rows x {ncol(data)} cols, ",
    "mem ~{format(object.size(data), units = 'Mb')}"
  )
  
  invisible(NULL)
}
