# ==============================================================================
# 60_features_elo.R
# Elo rating calculation for volleyball teams
# ==============================================================================

library(dplyr)
library(logger)


#' Calculate expected score based on Elo rating difference
#'
#' @param rating_a Elo rating of team A
#' @param rating_b Elo rating of team B
#' @return Expected score for team A (0 to 1)
elo_expected_score <- function(rating_a, rating_b) {
  1 / (1 + 10^((rating_b - rating_a) / 400))
}


#' Update Elo rating after a match
#'
#' @param rating Current rating
#' @param expected Expected score (0 to 1)
#' @param actual Actual score (0 to 1, e.g., 1 for win, 0 for loss)
#' @param k K-factor (rating sensitivity, default 20)
#' @return New rating
elo_update_rating <- function(rating, expected, actual, k = 20) {
  rating + k * (actual - expected)
}


#' Compute Elo ratings for all teams across all matches
#'
#' @param matches Data frame with No, DateLocal, TeamNameA, TeamNameB, MatchPointsA, MatchPointsB
#' @param cfg Configuration list
#' @return Data frame with team_elo_history
compute_team_elo <- function(matches, cfg) {
  logger::log_info("Computing Elo ratings for teams...")
  
  if (is.null(matches) || nrow(matches) == 0) {
    logger::log_warn("No matches provided for Elo calculation")
    return(NULL)
  }
  
  base_rating <- cfg$elo$base_rating
  k_factor <- cfg$elo$k_factor
  
  # Initialize Elo ratings for all teams
  all_teams <- unique(c(matches$TeamNameA, matches$TeamNameB))
  elo_ratings <- setNames(rep(base_rating, length(all_teams)), all_teams)
  
  # Sort matches chronologically
  matches <- matches %>%
    dplyr::arrange(DateLocal, No)
  
  # Track history
  history <- list()
  
  for (i in seq_len(nrow(matches))) {
    match <- matches[i, ]
    
    team_a <- match$TeamNameA
    team_b <- match$TeamNameB
    
    # Skip if team names are missing
    if (is.na(team_a) || is.na(team_b)) {
      next
    }
    
    # Get current ratings
    rating_a <- elo_ratings[team_a]
    rating_b <- elo_ratings[team_b]
    
    # Expected scores
    exp_a <- elo_expected_score(rating_a, rating_b)
    exp_b <- 1 - exp_a
    
    # Determine actual outcome
    # If scores are available, use them; otherwise assume winner takes all
    if (!is.na(match$MatchPointsA) && !is.na(match$MatchPointsB)) {
      if (match$MatchPointsA > match$MatchPointsB) {
        actual_a <- 1
        actual_b <- 0
        winner <- team_a
      } else if (match$MatchPointsB > match$MatchPointsA) {
        actual_a <- 0
        actual_b <- 1
        winner <- team_b
      } else {
        # Draw (rare in volleyball)
        actual_a <- 0.5
        actual_b <- 0.5
        winner <- NA_character_
      }
    } else {
      # No score info, skip this match
      logger::log_debug("Match {match$No}: missing scores, skipping Elo update")
      next
    }
    
    # Update ratings
    new_rating_a <- elo_update_rating(rating_a, exp_a, actual_a, k_factor)
    new_rating_b <- elo_update_rating(rating_b, exp_b, actual_b, k_factor)
    
    # Record history for both teams
    history[[length(history) + 1]] <- data.frame(
      MatchNo = match$No,
      DateLocal = match$DateLocal,
      TeamName = team_a,
      Opponent = team_b,
      EloBefore = rating_a,
      EloAfter = new_rating_a,
      ExpectedScore = exp_a,
      ActualScore = actual_a,
      WinFlag = ifelse(is.na(winner), NA, winner == team_a),
      stringsAsFactors = FALSE
    )
    
    history[[length(history) + 1]] <- data.frame(
      MatchNo = match$No,
      DateLocal = match$DateLocal,
      TeamName = team_b,
      Opponent = team_a,
      EloBefore = rating_b,
      EloAfter = new_rating_b,
      ExpectedScore = exp_b,
      ActualScore = actual_b,
      WinFlag = ifelse(is.na(winner), NA, winner == team_b),
      stringsAsFactors = FALSE
    )
    
    # Update current ratings
    elo_ratings[team_a] <- new_rating_a
    elo_ratings[team_b] <- new_rating_b
  }
  
  # Combine all history
  if (length(history) == 0) {
    logger::log_warn("No Elo history generated")
    return(NULL)
  }
  
  elo_history <- dplyr::bind_rows(history)
  
  log_data_summary(elo_history, "team_elo_history")
  
  elo_history
}


#' Compute upset metrics (underdog wins)
#'
#' @param elo_history Team Elo history data frame
#' @return Data frame with upsets
compute_upsets <- function(elo_history) {
  logger::log_info("Computing upsets...")
  
  if (is.null(elo_history) || nrow(elo_history) == 0) {
    logger::log_warn("No Elo history provided for upset calculation")
    return(NULL)
  }
  
  # Filter to only winning teams
  upsets <- elo_history %>%
    dplyr::filter(WinFlag == TRUE) %>%
    dplyr::mutate(
      # Surprise index: 1 - expected score (higher = more surprising)
      SurpriseIndex = 1 - ExpectedScore,
      IsUpset = SurpriseIndex > 0.5  # Won as underdog
    ) %>%
    dplyr::filter(IsUpset) %>%
    dplyr::arrange(dplyr::desc(SurpriseIndex)) %>%
    dplyr::select(
      MatchNo,
      DateLocal,
      Winner = TeamName,
      Loser = Opponent,
      WinnerEloBefore = EloBefore,
      WinnerEloAfter = EloAfter,
      ExpectedWinProb = ExpectedScore,
      SurpriseIndex
    )
  
  log_data_summary(upsets, "upsets")
  
  upsets
}


#' Write Elo history and upsets to warehouse
#'
#' @param conn DuckDB connection
#' @param elo_history Elo history data frame
#' @param upsets Upsets data frame
write_elo_to_warehouse <- function(conn, elo_history, upsets) {
  if (!is.null(elo_history)) {
    logger::log_info("Writing team_elo_history to warehouse...")
    DBI::dbWriteTable(conn, "team_elo_history", elo_history, overwrite = TRUE)
  }
  
  if (!is.null(upsets)) {
    logger::log_info("Writing upsets to warehouse...")
    DBI::dbWriteTable(conn, "upsets", upsets, overwrite = TRUE)
  }
  
  invisible(NULL)
}
