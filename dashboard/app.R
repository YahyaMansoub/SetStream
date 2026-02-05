# ==============================================================================
# app.R
# Shiny dashboard for SetStream volleyball analytics
# ==============================================================================

library(shiny)
library(shinydashboard)
library(DBI)
library(duckdb)
library(dplyr)
library(DT)
library(plotly)
library(logger)

# Load config
source("../R/00_utils.R")
cfg <- load_config()

# Get database connection
get_conn <- function() {
  tryCatch(
    {
      DBI::dbConnect(
        duckdb::duckdb(),
        dbdir = cfg$storage$warehouse_path,
        read_only = TRUE
      )
    },
    error = function(e) {
      NULL
    }
  )
}

# ==============================================================================
# UI
# ==============================================================================

ui <- dashboardPage(
  skin = "blue",
  
  dashboardHeader(title = "SetStream 🏐"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Top Teams", tabName = "top_teams", icon = icon("trophy")),
      menuItem("Team Detail", tabName = "team_detail", icon = icon("chart-line")),
      menuItem("Upsets", tabName = "upsets", icon = icon("bolt")),
      menuItem("Pipeline Status", tabName = "pipeline", icon = icon("cogs"))
    )
  ),
  
  dashboardBody(
    tabItems(
      # Top Teams Tab
      tabItem(
        tabName = "top_teams",
        h2("Top Teams by Elo Rating"),
        fluidRow(
          box(
            title = "Elo Leaderboard",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            DT::dataTableOutput("top_teams_table")
          )
        )
      ),
      
      # Team Detail Tab
      tabItem(
        tabName = "team_detail",
        h2("Team Detail"),
        fluidRow(
          box(
            title = "Select Team",
            status = "info",
            width = 3,
            selectInput("selected_team", "Team:", choices = NULL)
          ),
          box(
            title = "Current Elo",
            status = "success",
            width = 3,
            h3(textOutput("team_current_elo"))
          ),
          box(
            title = "Recent Form",
            status = "warning",
            width = 3,
            h3(textOutput("team_form"))
          ),
          box(
            title = "Last Match",
            status = "info",
            width = 3,
            h4(textOutput("team_last_match"))
          )
        ),
        fluidRow(
          box(
            title = "Elo History",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            plotlyOutput("elo_chart", height = "400px")
          )
        ),
        fluidRow(
          box(
            title = "Recent Matches",
            status = "primary",
            width = 12,
            DT::dataTableOutput("team_matches_table")
          )
        )
      ),
      
      # Upsets Tab
      tabItem(
        tabName = "upsets",
        h2("Recent Upsets"),
        fluidRow(
          box(
            title = "Filters",
            status = "info",
            width = 3,
            sliderInput("upset_days", "Days back:", min = 7, max = 365, value = 90)
          )
        ),
        fluidRow(
          box(
            title = "Upsets",
            status = "danger",
            solidHeader = TRUE,
            width = 12,
            DT::dataTableOutput("upsets_table")
          )
        )
      ),
      
      # Pipeline Status Tab
      tabItem(
        tabName = "pipeline",
        h2("Pipeline Status"),
        fluidRow(
          box(
            title = "Database Status",
            status = "success",
            width = 4,
            verbatimTextOutput("db_status")
          ),
          box(
            title = "Last Pipeline Run",
            status = "info",
            width = 4,
            verbatimTextOutput("last_run")
          ),
          box(
            title = "Data Summary",
            status = "warning",
            width = 4,
            verbatimTextOutput("data_summary")
          )
        )
      )
    )
  )
)

# ==============================================================================
# Server
# ==============================================================================

server <- function(input, output, session) {
  
  # Reactive connection
  conn <- reactive({
    invalidateLater(60000)  # Refresh every 60 seconds
    get_conn()
  })
  
  # Top teams data
  top_teams_data <- reactive({
    req(conn())
    
    sql <- "
      SELECT
          TeamName,
          CurrentElo,
          LastMatchDate
      FROM (
          SELECT
              TeamName,
              EloAfter AS CurrentElo,
              DateLocal AS LastMatchDate,
              ROW_NUMBER() OVER (PARTITION BY TeamName ORDER BY DateLocal DESC, MatchNo DESC) AS rn
          FROM team_elo_history
      )
      WHERE rn = 1
      ORDER BY CurrentElo DESC
      LIMIT 50
    "
    
    tryCatch(
      DBI::dbGetQuery(conn(), sql),
      error = function(e) {
        data.frame(TeamName = character(0), CurrentElo = numeric(0), LastMatchDate = character(0))
      }
    )
  })
  
  # Update team selector
  observe({
    teams <- top_teams_data()
    if (!is.null(teams) && nrow(teams) > 0) {
      updateSelectInput(session, "selected_team", choices = teams$TeamName)
    }
  })
  
  # Top teams table
  output$top_teams_table <- DT::renderDataTable({
    teams <- top_teams_data()
    
    teams %>%
      mutate(
        CurrentElo = round(CurrentElo, 1),
        Rank = row_number()
      ) %>%
      select(Rank, TeamName, CurrentElo, LastMatchDate) %>%
      DT::datatable(
        options = list(pageLength = 25, dom = 'ftp'),
        rownames = FALSE
      )
  })
  
  # Team Elo history
  team_elo_history <- reactive({
    req(input$selected_team, conn())
    
    sql <- "
      SELECT *
      FROM mart_team_elo_history
      WHERE TeamName = ?
      ORDER BY DateLocal
    "
    
    tryCatch(
      DBI::dbGetQuery(conn(), sql, params = list(input$selected_team)),
      error = function(e) {
        data.frame()
      }
    )
  })
  
  # Current Elo
  output$team_current_elo <- renderText({
    history <- team_elo_history()
    if (nrow(history) > 0) {
      round(tail(history$EloAfter, 1), 1)
    } else {
      "N/A"
    }
  })
  
  # Recent form
  output$team_form <- renderText({
    history <- team_elo_history()
    if (nrow(history) >= 5) {
      recent <- tail(history, 5)
      wins <- sum(recent$WinFlag, na.rm = TRUE)
      paste0(wins, "W-", 5 - wins, "L")
    } else {
      "Insufficient data"
    }
  })
  
  # Last match
  output$team_last_match <- renderText({
    history <- team_elo_history()
    if (nrow(history) > 0) {
      format(as.Date(tail(history$DateLocal, 1)), "%Y-%m-%d")
    } else {
      "N/A"
    }
  })
  
  # Elo chart
  output$elo_chart <- renderPlotly({
    history <- team_elo_history()
    
    if (nrow(history) == 0) {
      return(plotly_empty())
    }
    
    plot_ly(history, x = ~DateLocal, y = ~EloAfter, type = 'scatter', mode = 'lines+markers',
            marker = list(size = 5),
            line = list(width = 2),
            hovertemplate = paste(
              '<b>Date:</b> %{x}<br>',
              '<b>Elo:</b> %{y:.1f}<br>',
              '<extra></extra>'
            )) %>%
      layout(
        xaxis = list(title = "Date"),
        yaxis = list(title = "Elo Rating"),
        hovermode = "closest"
      )
  })
  
  # Team matches table
  output$team_matches_table <- DT::renderDataTable({
    history <- team_elo_history()
    
    if (nrow(history) == 0) {
      return(DT::datatable(data.frame()))
    }
    
    history %>%
      arrange(desc(DateLocal)) %>%
      mutate(
        Result = ifelse(WinFlag, "Win", "Loss"),
        EloChange = round(EloChange, 1)
      ) %>%
      select(DateLocal, Opponent, Result, EloChange, TournamentName) %>%
      head(20) %>%
      DT::datatable(
        options = list(pageLength = 10, dom = 't'),
        rownames = FALSE
      )
  })
  
  # Upsets table
  output$upsets_table <- DT::renderDataTable({
    req(conn())
    
    sql <- glue::glue("
      SELECT
          DateLocal,
          Winner,
          Loser,
          SurpriseScore,
          TournamentName
      FROM mart_upsets
      WHERE DateLocal >= CURRENT_DATE - INTERVAL '{input$upset_days}' DAY
      ORDER BY SurpriseScore DESC
      LIMIT 100
    ")
    
    upsets <- tryCatch(
      DBI::dbGetQuery(conn(), sql),
      error = function(e) {
        data.frame()
      }
    )
    
    if (nrow(upsets) > 0) {
      upsets %>%
        mutate(SurpriseScore = round(SurpriseScore, 1)) %>%
        DT::datatable(
          options = list(pageLength = 25, dom = 'ftp'),
          rownames = FALSE
        )
    } else {
      DT::datatable(data.frame())
    }
  })
  
  # Database status
  output$db_status <- renderText({
    c <- conn()
    if (!is.null(c) && DBI::dbIsValid(c)) {
      "✓ Connected"
    } else {
      "✗ Not Connected\nRun pipeline first"
    }
  })
  
  # Last run
  output$last_run <- renderText({
    state_file <- cfg$storage$state_path
    if (file.exists(state_file)) {
      state <- jsonlite::fromJSON(state_file)
      paste("Last run:", state$last_run)
    } else {
      "No pipeline runs yet"
    }
  })
  
  # Data summary
  output$data_summary <- renderText({
    req(conn())
    
    counts <- tryCatch(
      {
        list(
          tournaments = DBI::dbGetQuery(conn(), "SELECT COUNT(*) as n FROM stg_tournaments")$n,
          matches = DBI::dbGetQuery(conn(), "SELECT COUNT(*) as n FROM stg_matches")$n,
          teams = DBI::dbGetQuery(conn(), "SELECT COUNT(DISTINCT TeamName) as n FROM team_elo_history")$n
        )
      },
      error = function(e) {
        list(tournaments = 0, matches = 0, teams = 0)
      }
    )
    
    paste0(
      "Tournaments: ", counts$tournaments, "\n",
      "Matches: ", counts$matches, "\n",
      "Teams: ", counts$teams
    )
  })
  
  # Cleanup on stop
  onStop(function() {
    c <- conn()
    if (!is.null(c) && DBI::dbIsValid(c)) {
      DBI::dbDisconnect(c, shutdown = TRUE)
    }
  })
}

# Run the app
shinyApp(ui, server)
