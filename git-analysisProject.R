library(shiny)
library(shinydashboard)
library(shinyjs)
library(DT)
library(dplyr)
library(plotly)
library(tidyr)
library(ggplot2)
library(RColorBrewer)
library(treemap)
library(glue)
library(tools)
library(httr)
library(DBI)
library(duckdb)
library(stringr)
library(networkD3)
library(markdown)
library(zoo)
library(igraph)
library(lubridate)



scale_0_1 <- function(x) {
  if(length(unique(x)) <= 1 || (max(x) - min(x)) == 0) return(rep(0.5, length(x)))
  return((x - min(x)) / (max(x) - min(x)))
}

analyze_dev_productivity <- function(commit_history, diff_data) {
  commits_summary <- commit_history %>%
    mutate(date = as.Date(substr(date, 1, 10))) %>%
    group_by(author) %>%
    summarise(
      total_commits = n(),
      first_commit = min(date),
      last_commit = max(date),
      activity_duration = as.numeric(difftime(max(date), min(date), units = "days")) + 1,
      active_days = n_distinct(date),
      avg_daily_commits = total_commits / active_days,
      activity_frequency = active_days / activity_duration
    )
  
  code_changes <- diff_data %>%
    left_join(commit_history %>% select(commit, author, repo_id), by = c("commit"="commit", "repo_id"="repo_id")) %>%
    group_by(author) %>%
    summarise(
      files_changed = n_distinct(dst_file),
      lines_added = sum(count_add, na.rm = TRUE),
      lines_deleted = sum(count_del, na.rm = TRUE),
      net_contribution = lines_added - lines_deleted
    )
  
  productivity <- commits_summary %>%
    left_join(code_changes, by = "author") %>%
    mutate(
      avg_commit_size = (lines_added + lines_deleted) / total_commits,
      contribution_per_day = net_contribution / active_days
    ) %>%
    filter(total_commits >= 1) 
  
  return(productivity)
}

analyze_dev_expertise <- function(commit_history, diff_data) {
  language_usage <- diff_data %>%
    mutate(ext = tolower(tools::file_ext(dst_file)),
           language = case_when(
             ext %in% c("py", "ipynb") ~ "Python",
             ext %in% c("r", "rmd", "qmd") ~ "R",
             ext %in% c("js", "jsx", "ts", "tsx") ~ "JavaScript/TypeScript",
             ext %in% c("java") ~ "Java",
             ext %in% c("c", "cpp", "h", "hpp") ~ "C/C++",
             ext %in% c("go") ~ "Go",
             ext %in% c("php") ~ "PHP",
             ext %in% c("sql") ~ "SQL",
             ext %in% c("html", "htm") ~ "HTML",
             ext %in% c("css", "scss", "sass") ~ "CSS",
             TRUE ~ "Другое"
           )) %>%
    left_join(commit_history %>% select(commit, author, repo_id), by = c("commit"="commit", "repo_id"="repo_id")) %>%
    filter(!is.na(author)) %>%
    group_by(author, language) %>%
    summarise(
      commits = n_distinct(commit),
      lines = sum(count_add, na.rm = TRUE) + sum(count_del, na.rm = TRUE),
      .groups = "drop"
    )
  
  primary_languages <- language_usage %>%
    group_by(author) %>%
    arrange(desc(commits)) %>%
    slice(1) %>%
    select(author, primary_language = language)
  
  expertise_scores <- language_usage %>%
    group_by(language) %>%
    mutate(
      language_commits = scale_0_1(commits),
      language_lines = scale_0_1(lines),
      language_score = language_commits * 0.6 + language_lines * 0.4
    ) %>%
    group_by(author) %>%
    summarise(
      expertise_score = max(language_score),
      languages = list(language[order(language_score, decreasing = TRUE)])
    )
  
  combined_expertise <- expertise_scores %>%
    left_join(primary_languages, by = "author")
  
  return(combined_expertise)
}

generate_team_recommendations <- function(commit_history, diff_data, project_requirements) {
  
  productivity <- analyze_dev_productivity(commit_history, diff_data)
  
  languages_expertise <- diff_data %>%
    mutate(ext = tolower(tools::file_ext(dst_file)),
           language = case_when(
             ext %in% c("py", "ipynb") ~ "Python",
             ext %in% c("r", "rmd", "qmd") ~ "R",
             ext %in% c("js", "jsx", "ts", "tsx") ~ "JavaScript/TypeScript",
             ext %in% c("java") ~ "Java",
             ext %in% c("c", "cpp", "h", "hpp") ~ "C/C++",
             ext %in% c("go") ~ "Go",
             ext %in% c("rb") ~ "Ruby",
             ext %in% c("php") ~ "PHP",
             ext %in% c("sql") ~ "SQL",
             ext %in% c("html", "htm") ~ "HTML",
             ext %in% c("css", "scss", "sass") ~ "CSS",
             ext %in% c("md", "docx", "txt", "tex") ~ "Документация",
             TRUE ~ NA_character_
           )) %>%
    filter(!is.na(language)) %>%
    left_join(commit_history %>% select(commit, author, repo_id), by = c("commit"="commit", "repo_id"="repo_id")) %>%
    group_by(author, language) %>%
    summarise(
      commits = n_distinct(commit),
      lines = sum(count_add, na.rm = TRUE) + sum(count_del, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    group_by(language) %>%
    mutate(
      score = scale_0_1(commits) * 0.7 + scale_0_1(lines) * 0.3
    ) %>%
    ungroup()
  
  backend_langs <- c("Python", "R", "Java", "C/C++", "Go", "Ruby", "PHP", "SQL")
  frontend_langs <- c("JavaScript/TypeScript", "HTML", "CSS")
  docs_langs <- c("Документация", "R", "rmd", "qmd", "md", "docx", "txt", "tex")
  
  if (project_requirements$type == "backend") {
    languages_expertise <- languages_expertise %>%
      filter(language %in% backend_langs)
  } else if (project_requirements$type == "frontend") {
    languages_expertise <- languages_expertise %>%
      filter(language %in% frontend_langs)
  } else if (project_requirements$type == "docs") {
    languages_expertise <- languages_expertise %>%
      filter(language %in% docs_langs)
  }
  
  primary_languages <- languages_expertise %>%
    group_by(author) %>%
    arrange(desc(score)) %>%
    slice(1) %>%
    ungroup() %>%
    select(author, primary_language = language, primary_language_score = score)
  
  
  languages_used <- languages_expertise %>%
    filter(score > 0.3) %>%  
    group_by(author) %>%
    summarise(languages = paste(language, collapse = ", "))
  
  
  regularity <- commit_history %>%
    mutate(date = as.Date(substr(date, 1, 10))) %>%
    group_by(author) %>%
    summarise(
      first_commit = min(date),
      last_commit = max(date),
      active_period = as.numeric(difftime(max(date), min(date), units = "days")) + 1,
      active_days = n_distinct(date),
      regularity = active_days / active_period,
      recency_score = scale_0_1(as.numeric(difftime(Sys.Date(), last_commit, units = "days")) * -1 + 100)
    )
  
  team_scoring <- productivity %>%
    left_join(primary_languages, by = "author") %>%
    left_join(languages_used, by = "author") %>%
    left_join(regularity, by = "author") %>%
    rowwise() %>%
    mutate(
      
      experience_score = (log(total_commits + 1) * 0.4) + (scale_0_1(activity_duration) * 0.3) + (scale_0_1(lines_added + lines_deleted) * 0.3),
      
      
      activity_score = (scale_0_1(avg_daily_commits) * 0.6 + 
                          scale_0_1(contribution_per_day) * 0.2 + 
                          regularity * 0.2) * 10,
      
      
      total_score = case_when(
        project_requirements$priority == "experience" ~ 
          experience_score * 0.8 + activity_score * 0.1 + recency_score * 0.1,
        project_requirements$priority == "stability" ~ 
          experience_score * 0.3 + activity_score * 0.2 + regularity * 10 * 0.5,
        project_requirements$priority == "speed" ~ 
          experience_score * 0.3 + activity_score * 0.6 + recency_score * 0.1,
        TRUE ~ experience_score * 0.4 + activity_score * 0.4 + recency_score * 0.2
      )
    ) %>%
    ungroup() %>%
    arrange(desc(total_score))
  
  if (project_requirements$type == "fullstack") {
    backend_devs <- team_scoring %>%
      filter(primary_language %in% backend_langs) %>%
      head(1)
    
    frontend_devs <- team_scoring %>%
      filter(primary_language %in% frontend_langs) %>%
      head(1)
    
    other_devs <- team_scoring %>%
      filter(!(author %in% c(backend_devs$author, frontend_devs$author))) %>%
      head(max(3, project_requirements$suggested_team_size - 2))
    
    team_scoring <- bind_rows(backend_devs, frontend_devs, other_devs) %>%
      arrange(desc(total_score))
  }
  
  language_experts <- languages_expertise %>%
    filter(score > 0.7) %>%  
    group_by(language) %>%
    top_n(3, score) %>%
    summarise(top_experts = list(author[order(score, decreasing = TRUE)])) %>%
    rowwise() %>%
    mutate(top_experts = paste(unlist(top_experts), collapse = ", "))
  
  suggested_team_size <- min(
    max(3, ceiling(project_requirements$estimated_size / 50)),  
    max(2, floor(sqrt(project_requirements$estimated_size))),   
    nrow(team_scoring) 
  )
  
  list(
    team_scoring = team_scoring,
    language_experts = language_experts,
    suggested_team_size = suggested_team_size,
    productivity = productivity
  )
}


options(warn = -1)

ui <- dashboardPage(
  dashboardHeader(title = "Git Repository Analysis"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Репозитории", tabName = "repos", icon = icon("code-branch")),
      menuItem("Авторы", tabName = "authors", icon = icon("user")),
      menuItem("Обзор организации", tabName = "overview", icon = icon("code-branch")),
      menuItem("Рекомендации", tabName = "recommendations", icon = icon("lightbulb")),
      menuItem("О программе", tabName = "about", icon = icon("info-circle"))
    )
  ),
  
  dashboardBody(
    useShinyjs(),
    tags$head(
      tags$style(HTML("
        .small-box {margin-bottom: 15px;}
        .recommendation-item {
          background: #f8f9fa;
          padding: 15px;
          border-radius: 5px;
          margin-bottom: 20px;
          border-left: 4px solid #3c8dbc;
        }
        .recommendation-item h4 {
          margin-top: 0;
          color: #3c8dbc;
        }
        .text-warning {
          color: #f39c12;
        }
        .developer-card {
          padding: 10px;
          margin-bottom: 10px;
          background: #f4f6f9;
          border-radius: 3px;
          box-shadow: 0 1px 1px rgba(0,0,0,0.1);
        }
        .developer-card-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 5px;
          border-bottom: 1px solid #e9ecef;
          padding-bottom: 5px;
        }
        .score-badge {
          background: #3c8dbc;
          color: white;
          padding: 2px 6px;
          border-radius: 3px;
          font-weight: bold;
        }
        #loading-overlay {
          position: fixed;
          top: 0;
          left: 0;
          width: 100%;
          height: 100%;
          background-color: rgba(255, 255, 255, 0.7);
          z-index: 9999;
          display: none;
          justify-content: center;
          align-items: center;
          flex-direction: column;
        }
        .spinner {
          width: 80px;
          height: 80px;
          border-radius: 50%;
          border: 6px solid #f3f3f3;
          border-top: 6px solid #3498db;
          animation: spin 1s linear infinite;
        }
        .loading-text {
          margin-top: 20px;
          font-size: 18px;
          font-weight: bold;
        }
        @keyframes spin {
          0% { transform: rotate(0deg); }
          100% { transform: rotate(360deg); }
        }
      "))
    ),
    tags$div(
      id = "loading-overlay",
      tags$div(class = "spinner"),
      tags$div(class = "loading-text", "Обработка данных...")
    ),
    tabItems(
      tabItem(tabName = "repos",
              fluidRow(
                box(
                  title = "Добавить репозиторий", width = 12, status = "primary",
                  radioButtons("mode", "Выберите режим:",
                               choices = list("Локальный репозиторий" = 0,
                                              "Удаленный репозиторий" = 1,
                                              "Имя пользователя GitHub" = 2),
                               inline = TRUE),
                  
                  conditionalPanel(
                    condition = "input.mode == 0",
                    textInput("repo_local_dir", "Путь к локальному репозиторию:", 
                              placeholder = "D:/examle")
                  ),
                  
                  conditionalPanel(
                    condition = "input.mode == 1",
                    textInput("repo_url", "URL удаленного репозитория с .git:", 
                              placeholder = "https://github.com/username/example.git"),
                    textInput("clone_dir", "Директория для клонирования:", 
                              placeholder = "examleDir")
                  ),
                  
                  conditionalPanel(
                    condition = "input.mode == 2",
                    textInput("username", "Имя пользователя GitHub:", 
                              placeholder = "username"),
                    textInput("clone_dir2", "Директория для клонирования:", 
                              placeholder = "examleDir")
                  ),
                  
                  actionButton("submit", "Добавить в базу данных", 
                               class = "btn-primary", icon = icon("play"))
                )
              ),
              fluidRow(
                box(
                  title = "Список репозиториев", width = 12, status = "primary",
                  DTOutput("repo_data"),
                  br(),
                  actionButton("analyze_repo", "Анализировать", class = "btn-success"),
                  actionButton("delete_repo", "Удалить", class = "btn-danger", icon = icon("trash")),
                  downloadButton("download_repos", "Скачать данные")
                )
              )
      ),
      
      tabItem(tabName = "authors",
              fluidRow(
                box(
                  title = "Список авторов", width = 12, status = "primary",
                  DTOutput("author_data"),
                  br(),
                  actionButton("analyze_author", "Анализировать", class = "btn-success"),
                  downloadButton("download_authors", "Скачать данные")
                )
              )
      ),
      
      tabItem(tabName = "overview",
              fluidRow(
                box(
                  title = "Обзор активности проекта", width = 12, status = "primary",
                  plotlyOutput("project_dynamics", height = "300px")
                )
              ),
              fluidRow(
                column(width = 4,
                       box(title = "Лучшие разработчики", width = NULL, status = "primary",
                           uiOutput("top_developers")
                       )
                ),
                column(width = 8,
                       box(title = "Сеть взаимодействия разработчиков", width = NULL, status = "primary",
                           forceNetworkOutput("collaboration_network", height = "500px")
                       )
                )
              )
      ),
      tabItem(tabName = "recommendations",
              fluidRow(
                box(
                  title = "Параметры проекта", width = 12, status = "primary",
                  column(width = 4,
                         numericInput("estimated_size", "Примерный размер проекта (KLOC):", 50, min = 1, max = 1000)
                  ),
                  column(width = 4,
                         selectInput("project_priority", "Приоритет проекта:",
                                     choices = c("Опыт" = "experience", 
                                                 "Скорость разработки" = "speed", 
                                                 "Стабильность" = "stability", 
                                                 "Баланс" = "balanced")),
                         selectInput("project_type", "Тип проекта:",
                                     choices = c("Backend" = "backend",
                                                 "Frontend" = "frontend", 
                                                 "Fullstack" = "fullstack",
                                                 "Документация" = "docs"))
                  ),
                  column(width = 4,
                         actionButton("generate_recommendations", "Сформировать рекомендации", 
                                      class = "btn-primary", style = "margin-top: 25px;")
                  )
                )
              ),
              fluidRow(
                uiOutput("recommendations_output")
              )
      ),
      tabItem(tabName = "about",
              box(
                title = "О программе", width = 12, status = "primary",
                includeMarkdown("README.md")
              )
      )
    ),
    tags$script(HTML("
      function showLoading() {
        document.getElementById('loading-overlay').style.display = 'flex';
      }
      
      function hideLoading() {
        document.getElementById('loading-overlay').style.display = 'none';
      }
      
      Shiny.addCustomMessageHandler('show_loading', function(message) {
        showLoading();
      });
      
      Shiny.addCustomMessageHandler('hide_loading', function(message) {
        hideLoading();
      });
    "))
  )
)

server <- function(input, output, session) {
  
  values <- reactiveValues(
    status = "",
    repo_path = NULL,
    git_commit_history = NULL,
    git_diff = NULL,
    authors = NULL,  
    selected_author = NULL,
    selected_repo_id = NULL,
    selected_repo_name = NULL,
    date_range = NULL,
    productivity_data = NULL,
    team_recommendations = NULL
  )
  
  loadData <- function() {
    con <- dbConnect(duckdb(), dbdir = "git.duckdb")
    values$repo_path <- dbGetQuery(con, "SELECT * FROM repo_path")
    values$git_commit_history <- dbGetQuery(con, "SELECT * FROM git_commit_history LIMIT 10000")
    values$git_diff <- dbGetQuery(con, "SELECT * FROM git_diff LIMIT 10000")
    dbDisconnect(con)
    
    values$git_commit_history <- values$git_commit_history %>%
      mutate(date = as.character(date))
    
    updateSelectInput(session, "developer_forecast_select", 
                      choices = unique(values$git_commit_history$author))
    
    if(!is.null(values$git_commit_history) && nrow(values$git_commit_history) > 0) {
      values$authors <- values$git_commit_history %>%
        group_by(author) %>%
        summarise(commits = n(), 
                  first_commit = format(as.POSIXct(min(date)), "%d.%m.%Y %H:%M"), 
                  last_commit = format(as.POSIXct(max(date)), "%d.%m.%Y %H:%M"),
                  .groups = "drop")
    }
  }
  
  
  observeEvent(input$submit, {
    session$sendCustomMessage(type = "show_loading", message = list())
    
    params <- switch(as.character(input$mode),
                     "0" = list(mode = 0, repo_local_dir = input$repo_local_dir),
                     "1" = list(mode = 1, repo_url = input$repo_url, clone_dir = input$clone_dir),
                     "2" = list(mode = 2, username = input$username, clone_dir = input$clone_dir2)
    )
    
    withCallingHandlers(
      {
        tryCatch({
          source("ETL.R")
          result <- do.call(run_etl_pipeline, params)
          
          if (result$status == "success") {
            showNotification("Данные успешно загружены!", type = "message", duration = 5)
            loadData()
          } else {
            showNotification(glue("\nОшибка: {result$message}"), type = "error", duration = 5)
          }
          loadData()
          
        }, finally = {
          session$sendCustomMessage(type = "hide_loading", message = list())
        })
      }
    )
  })
  
  output$status_message <- renderText({
    values$status
  })
  
  output$repo_data <- renderDT({
    req(values$repo_path)
    datatable(values$repo_path, selection = 'single')
  })
  
  output$author_data <- renderDT({
    req(values$authors)
    datatable(values$authors, selection = 'single')
  })
  
  observeEvent(input$author_data_rows_selected, {
    selected_row <- input$author_data_rows_selected
    if(length(selected_row) > 0) {
      values$selected_author <- values$authors$author[selected_row]
    }
  })
  
  observeEvent(input$repo_data_rows_selected, {
    selected_row <- input$repo_data_rows_selected
    if(length(selected_row) > 0) {
      values$selected_repo_id <- values$repo_path$id[selected_row]
      values$selected_repo_name <- values$repo_path$repo[selected_row]
    }
  })
  
  
  observeEvent(input$delete_repo, {
    req(values$selected_repo_id, values$repo_path)
    
    showModal(modalDialog(
      title = paste("Удаление репозитория:", values$selected_repo),
      size = "m",
      easyClose = TRUE,
      "Вы уверены, что хотите удалить репозиторий из базы данных?",
      footer = tagList(
        modalButton("Отмена"),
        actionButton("confirm_delete", "Удалить", class = "btn-danger")
      )
    ))
  })
  
  observeEvent(input$confirm_delete, {
    req(values$selected_repo_id, values$repo_path)
    
    repo_info <- values$repo_path %>% 
      filter(id == values$selected_repo_id)
    
    if (nrow(repo_info) == 0) {
      showNotification("Репозиторий не найден в базе данных", type = "error")
      removeModal()
      return()
    }
    
    con <- dbConnect(duckdb(), dbdir = "git.duckdb")
    
    tryCatch({
      dbExecute(con, glue("DELETE FROM git_commit_history WHERE repo = '{values$selected_repo_id}'"))
      dbExecute(con, glue("DELETE FROM git_diff WHERE repo = '{values$selected_repo_id}'"))
      dbExecute(con, glue("DELETE FROM repo_path WHERE id = '{values$selected_repo_id}'"))
      
      values$repo_path <- values$repo_path %>% 
        filter(id != values$selected_repo_id)
      
      if (!is.null(values$git_commit_history)) {
        values$git_commit_history <- values$git_commit_history %>% 
          filter(repo_id != values$selected_repo_id)
      }
      
      if (!is.null(values$git_diff)) {
        values$git_diff <- values$git_diff %>% 
          filter(repo_id != values$selected_repo_id)
      }
      
      if (!is.null(values$git_commit_history) && nrow(values$git_commit_history) > 0) {
        values$authors <- values$git_commit_history %>%
          group_by(author) %>%
          summarise(
            commits = n(), 
            first_commit = if (n() > 0) format(as.POSIXct(min(date)), "%d.%m.%Y %H:%M") else NA_character_,
            last_commit = if (n() > 0) format(as.POSIXct(max(date)), "%d.%m.%Y %H:%M") else NA_character_,
            .groups = "drop"
          )
      } else {
        values$authors <- NULL
      }
      
      showNotification("Репозиторий успешно удален из базы данных", type = "message")
      removeModal()
      
    }, error = function(e) {
      showNotification(paste("Ошибка при удалении:", e$message), type = "error")
    }, finally = {
      dbDisconnect(con, shutdown = TRUE)
    })
  })
  
  
  filterDataByDateRange <- function(data, date_range, date_column = "date") {
    if (is.null(date_range)) {
      return(data)
    }
    
    data %>%
      filter(
        as.Date(substr(!!sym(date_column), 1, 10)) >= date_range[1] &
          as.Date(substr(!!sym(date_column), 1, 10)) <= date_range[2]
      )
  }
  
  createLanguageUsageChart <- function(git_diff_data, date_range = NULL) {
    if (!is.null(date_range) && !is.null(git_diff_data)) {
      commit_ids <- values$git_commit_history %>%
        filter(
          as.Date(substr(date, 1, 10)) >= date_range[1] &
            as.Date(substr(date, 1, 10)) <= date_range[2]
        ) %>%
        pull(commit)
      
      git_diff_data <- git_diff_data %>%
        filter(commit %in% commit_ids)
    }
    
    unique_files <- git_diff_data %>%
      filter(!is.na(dst_file),
             dst_file != "/dev/null",
             dst_file != "") %>%
      distinct(dst_file) %>%
      pull(dst_file)
    
    if (length(unique_files) == 0) {
      return(ggplot() + 
               annotate("text", x = 0, y = 0, label = "Нет данных для отображения") +
               theme_void())
    }
    
    extensions <- sapply(unique_files, function(file) {
      if (grepl("\\.", file)) {
        ext <- tolower(tools::file_ext(file))
      } else {
        ext <- "no_extension"
      }
      return(ext)
    })
    
    
    lang_count <- data.frame(ext = extensions) %>%
      filter(
        ext %in% c("py", "r","R", "js", "c", "cpp", "cs", "java")
      ) %>%
      group_by(ext) %>%
      summarise(count = n()) %>%
      mutate(language = case_when(
        ext == "py" ~ "Python",
        ext == "r" ~ "R",
        ext == "R" ~ "R",
        ext == "js" ~ "JavaScript",
        ext == "c" ~ "C",
        ext == "cpp" ~ "C++",
        ext == "cs" ~ "C#",
        ext == "java" ~ "Java",
        TRUE ~ ext
      ))
    
    lang_count <- lang_count %>%
      group_by(language) %>%
      summarise(count = sum(count))
    
    if(nrow(lang_count) > 0) {
      lang_count <- lang_count %>%
        mutate(
          percent = count / sum(count) * 100,
          label = paste0(language, "\n", round(count), " (", round(percent, 1), "%)")
        )
      n_lang <- nrow(lang_count)
      if (n_lang < 3) {
        fill_colors <- brewer.pal(3, "Set3")[1:n_lang]
      } else {
        fill_colors <- brewer.pal(n_lang, "Set3")
      }
      
      p <- ggplot(lang_count, aes(x = "", y = count, fill = language)) +
        geom_bar(stat = "identity", width = 1) +
        coord_polar("y", start = 0) +
        geom_text(aes(label = label), position = position_stack(vjust = 0.5)) +
        labs(title = "Использование языков программирования") +
        scale_fill_manual(values = fill_colors, name = "Язык") +
        theme_minimal() +
        theme(
          axis.title = element_blank(),
          axis.text = element_blank(),
          panel.grid = element_blank(),
          plot.title = element_text(hjust = 0.5, size = 16)
        )
      
      return(p)
    } else {
      return(ggplot() + 
               annotate("text", x = 0, y = 0, label = "Нет данных для отображения") +
               theme_void())
    }
  }
  
  createRepoTreemap <- function(git_diff_data, date_range = NULL) {
    if (!is.null(date_range) && !is.null(git_diff_data)) {
      commit_ids <- values$git_commit_history %>%
        filter(
          as.Date(substr(date, 1, 10)) >= date_range[1] &
            as.Date(substr(date, 1, 10)) <= date_range[2]
        ) %>%
        pull(commit)
      
      git_diff_data <- git_diff_data %>%
        filter(commit %in% commit_ids)
    }
    
    repo_files <- git_diff_data %>%
      filter(!is.na(dst_file)) %>%
      distinct(repo, dst_file) %>%
      group_by(repo) %>%
      summarise(files = n())
    
    if(nrow(repo_files) > 0) {
      n_repo <- nrow(repo_files)
      if (n_repo < 3) {
        repo_colors <- brewer.pal(3, "Set3")[1:n_repo]
      } else {
        repo_colors <- brewer.pal(n_repo, "Set3")
      }
      treemap_plot <- treemap(
        repo_files,
        index = "repo",
        vSize = "files",
        type = "index",
        title = "Анализ репозиториев по количеству файлов",
        fontsize.title = 14,
        fontsize.labels = 12,
        palette = repo_colors
      )
      
      return(treemap_plot)
    } else {
      plot(c(0, 1), c(0, 1), type = "n", axes = FALSE, xlab = "", ylab = "")
      text(0.5, 0.5, "Нет данных для отображения", cex = 1.5)
      return(NULL)
    }
  }
  
  createActivityHeatmap <- function(commit_data, date_range = NULL) {
    if (!is.null(date_range)) {
      commit_data <- commit_data %>%
        filter(
          as.Date(substr(date, 1, 10)) >= date_range[1] &
            as.Date(substr(date, 1, 10)) <= date_range[2]
        )
    }
    
    if (nrow(commit_data) == 0) {
      return(plot_ly() %>%
               add_annotations(
                 text = "Нет данных для отображения",
                 showarrow = FALSE,
                 font = list(size = 20)
               ))
    }
    
    heatmap_data <- commit_data %>%
      mutate(
        date_time = as.POSIXct(substr(date, 1, 19)),
        date = as.Date(date_time),
        weekday = factor(weekdays(date_time, abbreviate = TRUE),
                         levels = c("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")),
        hour = hour(date_time)
      ) %>%
      group_by(weekday, hour) %>%
      summarise(commits = n(), .groups = "drop") %>%
      complete(weekday, hour, fill = list(commits = 0))
    
    plot_ly(
      heatmap_data,
      x = ~hour,
      y = ~weekday,
      z = ~commits,
      type = "heatmap",
      colors = colorRamp(c("#f7fbff", "#4292c6", "#08306b")),
      hoverinfo = "text",
      text = ~paste("День:", weekday,
                    "<br>Час:", hour,
                    "<br>Коммитов:", commits)
    ) %>%
      layout(
        title = "Тепловая карта активности",
        xaxis = list(
          title = "Час дня",
          tickmode = "array",
          tickvals = 0:23
        ),
        yaxis = list(
          title = "День недели",
          autorange = "reversed"
        ),
        margin = list(l = 100, r = 50, b = 50, t = 50),
        font = list(family = "Arial", size = 12)
      )
  }
  
  
  createActiveHoursChart <- function(commit_data, date_range = NULL) {
    if (!is.null(date_range) && !is.null(commit_data)) {
      commit_data <- commit_data %>%
        filter(
          as.Date(substr(date, 1, 10)) >= date_range[1] &
            as.Date(substr(date, 1, 10)) <= date_range[2]
        )
    }
    
    if(nrow(commit_data) > 0) {
      hours_data <- commit_data %>%
        mutate(
          date_time = as.POSIXct(substr(date, 1, 19)),
          hour = hour(date_time)
        ) %>%
        group_by(hour) %>%
        summarise(commits = n(), .groups = "drop") %>%
        mutate(hour_label = paste0(hour, ":00"))
      
      p <- plot_ly(
        hours_data,
        x = ~factor(hour_label, levels = paste0(0:23, ":00")),
        y = ~commits,
        type = "bar",
        marker = list(color = "steelblue"),
        hoverinfo = "text",
        text = ~paste("Час:", hour_label, 
                      "<br>Количество коммитов:", commits)
      ) %>%
        layout(
          title = "Активность коммитов по часам",
          xaxis = list(
            title = "Час дня",
            tickangle = 45
          ),
          yaxis = list(
            title = "Количество коммитов"
          ),
          font = list(family = "Arial", size = 12)
        )
      
      return(p)
    } else {
      plot_ly() %>%
        add_annotations(
          text = "Нет данных для отображения",
          showarrow = FALSE,
          font = list(size = 20)
        )
    }
  }
  
  createCommitsByRepoChart <- function(commit_data, date_range = NULL) {
    if (!is.null(date_range) && !is.null(commit_data)) {
      commit_data <- commit_data %>%
        filter(
          as.Date(substr(date, 1, 10)) >= date_range[1] &
            as.Date(substr(date, 1, 10)) <= date_range[2]
        )
    }
    
    if(nrow(commit_data) > 0) {
      repo_commits <- commit_data %>%
        group_by(repo_id, repo) %>%
        summarise(commits = n(), .groups = "drop") %>%
        arrange(desc(commits))
      
      n <- nrow(repo_commits)
      if(n < 3) {
        colors <- brewer.pal(3, "Set3")[1:n]
      } else {
        colors <- brewer.pal(n, "Set3")
      }
      
      plot_ly(repo_commits, 
              x = ~repo, 
              y = ~commits, 
              type = "bar",
              marker = list(color = colors)) %>%
        layout(
          title = "Количество коммитов в репозиториях",
          xaxis = list(
            title = "",
            categoryorder = "total descending"
          ),
          yaxis = list(title = "Количество коммитов"),
          margin = list(b = 100),
          font = list(family = "Arial", size = 12)
        )
    } else {
      plot_ly() %>%
        add_annotations(
          text = "Нет данных для отображения",
          showarrow = FALSE,
          font = list(size = 20)
        )
    }
  }
  
  # Анализ выбранного автора
  observeEvent(input$analyze_author, {
    req(values$selected_author)
    
    author_data <- values$git_commit_history %>%
      filter(author == values$selected_author)
    
    if(nrow(author_data) == 0) {
      showNotification("Нет данных для выбранного автора", type = "error")
      return()
    }
    
    author_dates <- values$git_commit_history %>%
      filter(author == values$selected_author) %>%
      mutate(date_only = as.Date(substr(date, 1, 10))) %>%
      summarise(min_date = min(date_only), max_date = max(date_only))
    
    showModal(modalDialog(
      title = paste("Анализ:", values$selected_author),
      size = "l",
      easyClose = TRUE,
      
      fluidRow(
        column(
          width = 4,
          wellPanel(
            style = "height: 700px;",
            div(
              style = "text-align: center;",
              img(src = paste0("https://github.com/", values$selected_author, ".png"), 
                  height = 100, width = 100)
            ),
            br(),
            h4("Количество репозиториев"),
            textOutput("author_repo_count"),
            hr(),
            h4("Выбор исследуемого периода"),
            dateRangeInput("author_date_range", "", 
                           start = author_dates$min_date, 
                           end = author_dates$max_date,
                           min = author_dates$min_date,
                           max = author_dates$max_date, 
                           format = "dd.mm.yyyy",
                           separator = " - "),
            h4("Статистика:"),
            fluidRow(
              valueBoxOutput("total_commits_box", width = 6),
              valueBoxOutput("avg_commits_box", width = 6)
            ),
            fluidRow(
              valueBoxOutput("common_word_box", width = 6),
              valueBoxOutput("active_day_box", width = 6)
            )
          )
        ),
        column(
          width = 8,
          tabsetPanel(
            tabPanel("Использование языков программирования", plotOutput("author_lang_chart", height = "400px")),
            tabPanel("Анализ репозиториев по количеству файлов", plotOutput("author_treemap", height = "400px")),
            tabPanel("Тепловая карта", plotlyOutput("author_heatmap", height = "500px")),
            tabPanel("Коммиты в репозиториях", plotlyOutput("author_commits_chart", height = "400px")),
            tabPanel("Активные часы", plotlyOutput("author_active_hours", height = "400px"))
          )
        )
      )
    ))
    
    output$total_commits_box <- renderValueBox({
      req(values$selected_author)
      
      author_data <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      total_commits <- if(nrow(author_data) > 0) nrow(author_data) else "Нет данных"
      
      valueBox(
        value = tags$div(
          style = "text-align: center;",
          tags$div(icon("code"), style = "font-size: 12px; margin-bottom: 5px;"),
          tags$div(tags$b(total_commits), style = "font-size: 15px; font-weight: bold;")
        ),
        subtitle = tags$div(style = "text-align: center;", "коммитов"),
        width = 6
      )
    })
    
    output$avg_commits_box <- renderValueBox({
      req(values$selected_author)
      
      author_data <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      if(nrow(author_data) == 0) {
        avg_commits_per_day <- "Нет данных"
      } else {
        commits_per_day <- author_data %>%
          mutate(date_only = as.Date(substr(date, 1, 10))) %>%
          group_by(date_only) %>%
          summarise(daily_commits = n()) %>%
          pull(daily_commits)
        
        avg_commits_per_day <- if(length(commits_per_day) > 0) round(mean(commits_per_day), 1) else 0
      }
      
      valueBox(
        value = tags$div(
          style = "text-align: center;",
          tags$div(icon("calendar"), style = "font-size: 12px; margin-bottom: 5px;"),
          tags$div(tags$b(avg_commits_per_day), style = "font-size: 15px; font-weight: bold;")
        ),
        subtitle = tags$div(style = "text-align: center;", "коммитов в день"),
        width = 6
      )
    })
    
    output$common_word_box <- renderValueBox({
      req(values$selected_author)
      
      author_data <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      if(nrow(author_data) == 0) {
        most_common_word <- "Нет данных"
      } else {
        words <- unlist(strsplit(tolower(author_data$message), "\\W+"))
        word_freq <- table(words)
        word_freq <- word_freq[names(word_freq) != ""] 
        most_common_word <- if(length(word_freq) > 0) names(which.max(word_freq)) else "Нет данных"
      }
      
      valueBox(
        value = tags$div(
          style = "text-align: center;",
          tags$div(icon("comment"), style = "font-size: 12px; margin-bottom: 5px;"),
          tags$div(tags$b(most_common_word), style = "font-size: 15px; font-weight: bold;")
        ),
        subtitle = tags$div(style = "text-align: center;", "частое слово"),
        width = 6
      )
    })
    
    output$active_day_box <- renderValueBox({
      req(values$selected_author)
      
      author_data <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      if(nrow(author_data) == 0) {
        most_active_day <- "Нет данных"
      } else {
        weekday_activity <- author_data %>%
          mutate(weekday = weekdays(as.Date(substr(date, 1, 10)))) %>%
          count(weekday) %>%
          arrange(desc(n))
        
        most_active_day <- if(nrow(weekday_activity) > 0) weekday_activity$weekday[1] else "Нет данных"
      }
      
      valueBox(
        value = tags$div(
          style = "text-align: center;",
          tags$div(icon("star"), style = "font-size: 12px; margin-bottom: 5px;"),
          tags$div(tags$b(most_active_day), style = "font-size: 15px; font-weight: bold;")
        ),
        subtitle = tags$div(style = "text-align: center;", "активный день"),
        width = 6
      )
    })
    
    output$author_repo_count <- renderText({
      author_repos <- values$git_commit_history %>%
        filter(author == values$selected_author) %>%
        distinct(repo_id) %>%
        nrow()
      return(as.character(author_repos))
    })
    
    author_date_range <- reactive({
      input$author_date_range
    })
    
    output$author_lang_chart <- renderPlot({
      req(values$git_diff)
      
      author_commits <- values$git_commit_history %>%
        filter(author == values$selected_author) %>%
        pull(commit)
      
      git_diff_filtered <- values$git_diff %>%
        filter(commit %in% author_commits)
      
      createLanguageUsageChart(git_diff_filtered, author_date_range())
    })
    
    output$author_treemap <- renderPlot({
      req(values$git_diff)
      
      author_commits <- values$git_commit_history %>%
        filter(author == values$selected_author) %>%
        pull(commit)
      
      git_diff_filtered <- values$git_diff %>%
        filter(commit %in% author_commits)
      
      createRepoTreemap(git_diff_filtered, author_date_range())
    })
    
    output$author_heatmap <- renderPlotly({
      req(values$git_commit_history)
      
      commit_data_filtered <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      createActivityHeatmap(commit_data_filtered, author_date_range())
    })
    
    output$author_commits_chart <- renderPlotly({
      req(values$git_commit_history)
      
      commit_data_filtered <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      createCommitsByRepoChart(commit_data_filtered, author_date_range())
    })
    
    output$author_active_hours <- renderPlotly({
      req(values$git_commit_history)
      
      commit_data_filtered <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      createActiveHoursChart(commit_data_filtered, author_date_range())
    })
  })
  
  # Анализ выбранного репозитория
  observeEvent(input$analyze_repo, {
    req(values$selected_repo_id)
    
    repo_commits_data <- values$git_commit_history %>% 
      filter(repo_id == values$selected_repo_id)
    
    repo_diff_data <- values$git_diff %>% 
      filter(repo_id == values$selected_repo_id)
    
    top_authors <- repo_commits_data %>%
      group_by(author) %>%
      summarise(commits = n()) %>%
      arrange(desc(commits)) %>%
      head(10) %>%
      pull(author)
    
    date_range <- repo_commits_data %>%
      mutate(date = as.Date(substr(date, 1, 10))) %>%
      summarise(
        min_date = if(n() > 0) min(date) else as.Date(Sys.Date()),
        max_date = if(n() > 0) max(date) else as.Date(Sys.Date())
      )
    
    
    
    showModal(modalDialog(
      title = paste("Анализ репозитория:", values$selected_repo_name),
      size = "l",
      easyClose = TRUE,
      
      fluidRow(
        column(
          width = 3,
          wellPanel(
            h4(values$selected_repo_name),
            hr(),
            h4("Выбор участников"),
            selectizeInput(
              "repo_authors", 
              label = NULL,
              choices = unique(repo_commits_data$author),
              selected = top_authors,
              multiple = TRUE,
              options = list(
                plugins = list('remove_button', 'drag_drop'),
                placeholder = 'Поиск по имени...',
                persist = FALSE,
                create = TRUE
              )
            ),
            helpText("Начните вводить имя для поиска. Можно выбрать несколько авторов."),
            hr(),
            h4("Период исследования"),
            dateRangeInput(
              "repo_date_range",
              label = NULL,
              start = date_range$min_date,
              end = date_range$max_date,
              min = date_range$min_date,
              max = date_range$max_date, 
              format = "dd.mm.yyyy",
              separator = " - "
            )
          )
        ),
        column(
          width = 9,
          tabsetPanel(
            tabPanel("Вклад участников",
                     plotlyOutput("repo_contrib_chart", height = "500px")),
            tabPanel("Типы изменений",
                     plotlyOutput("repo_changes_chart", height = "500px")),
            tabPanel("История участия",
                     plotlyOutput("repo_history_chart", height = "500px")),
            tabPanel("Коммиты в файлах",
                     plotOutput("repo_file_tree", height = "500px"))
          )
        )
      )
    ))
    
    filtered_data <- reactive({
      req(input$repo_authors, input$repo_date_range)
      
      commits_filtered <- repo_commits_data %>%
        filter(
          author %in% input$repo_authors,
          as.Date(substr(date, 1, 10)) >= input$repo_date_range[1],
          as.Date(substr(date, 1, 10)) <= input$repo_date_range[2]
        )
      
      diff_filtered <- repo_diff_data %>%
        filter(commit %in% commits_filtered$commit)
      
      list(commits = commits_filtered, diff = diff_filtered)
    })
    
    output$repo_contrib_chart <- renderPlotly({
      data <- filtered_data()
      
      file_types <- data$diff %>%
        mutate(
          ext = tolower(file_ext(dst_file)),
          category = case_when(
            ext %in% c("r", "py", "c", "js", "java") ~ "Языки программирования",
            ext %in% c("md", "docx", "pdf", "qmd", "rmd", "pptx", "doc") ~ "Документация",
            TRUE ~ "Другое"
          )
        ) %>%
        group_by(commit) %>%
        distinct(commit, .keep_all = TRUE)
      
      contrib_data <- data$commits %>%
        left_join(file_types, by = "commit") %>%
        group_by(author, category) %>%
        summarise(count = n(), .groups = "drop")
      
      colors <- c("Языки программирования" = "#1f77b4", 
                  "Документация" = "#ff7f0e", 
                  "Другое" = "#2ca02c")
      
      plot_ly(contrib_data, 
              x = ~author, 
              y = ~count, 
              color = ~category,
              colors = colors,
              type = "bar") %>%
        layout(
          barmode = "stack",
          title = "Вклад участников проекта",
          xaxis = list(title = ""),
          yaxis = list(title = "Количество коммитов")
        )
    })
    
    output$repo_changes_chart <- renderPlotly({
      data <- filtered_data()
      
      changes_data <- data$diff %>%
        group_by(author = data$commits$author[match(commit, data$commits$commit)]) %>%
        summarise(
          add = sum(count_add, na.rm = TRUE),
          del = sum(count_del, na.rm = TRUE)
        )
      
      plot_ly(changes_data) %>%
        add_trace(
          x = ~author, 
          y = ~add, 
          name = "Добавлено",
          type = "bar"
        ) %>%
        add_trace(
          x = ~author, 
          y = ~del, 
          name = "Удалено",
          type = "bar"
        ) %>%
        layout(
          barmode = "group",
          title = "Типы изменений",
          xaxis = list(title = ""),
          yaxis = list(title = "Количество изменений")
        )
    })
    
    output$repo_history_chart <- renderPlotly({
      data <- filtered_data()
      
      history_data <- data$commits %>%
        mutate(date = as.Date(substr(date, 1, 10))) %>%
        group_by(date, author) %>%
        summarise(count = n(), .groups = "drop")
      
      plot_ly(history_data, 
              x = ~date, 
              y = ~count, 
              color = ~author,
              type = "scatter",
              mode = "lines+markers") %>%
        layout(
          title = "История участия",
          xaxis = list(title = "Дата"),
          yaxis = list(title = "Коммитов в день")
        )
    })
    
    output$repo_file_tree <- renderPlot({
      data <- filtered_data()
      
      file_data <- data$diff %>%
        group_by(dst_file) %>%
        summarise(commits = n_distinct(commit)) %>% 
        filter(!is.na(dst_file))
      
      if(nrow(file_data) > 0) {
        n_files <- nrow(file_data)
        if(n_files < 3) {
          file_colors <- brewer.pal(3, "Set3")[1:n_files]
        } else {
          file_colors <- brewer.pal(n_files, "Set3")
        }
        treemap(file_data,
                index = "dst_file",
                vSize = "commits",
                title = "Количество коммитов в файлах",
                palette = file_colors)
      } else {
        plot.new()
        text(0.5, 0.5, "Нет данных для отображения", cex = 1.5)
      }
    })
  })
  
  # Скачивание данных
  output$download_repos <- downloadHandler(
    filename = function() {
      paste("repos-", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(values$repo_path, file, row.names = FALSE)
    }
  )
  
  output$download_authors <- downloadHandler(
    filename = function() {
      paste("authors-", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      req(values$authors)
      write.csv(values$authors, file, row.names = FALSE)
    }
  )
  
  output$top_developers <- renderUI({
    req(values$git_commit_history, values$git_diff)
    
    if(is.null(values$productivity_data)) {
      values$productivity_data <- analyze_dev_productivity(values$git_commit_history, values$git_diff)
    }
    
    top_devs <- values$productivity_data %>%
      mutate(
        experience_score = (log(total_commits + 1) * 0.4) + (scale_0_1(activity_duration) * 0.3) + (scale_0_1(lines_added + lines_deleted) * 0.3),
        activity_score = (scale_0_1(avg_daily_commits) * 0.6 + scale_0_1(contribution_per_day) * 0.2 + scale_0_1(activity_frequency) * 0.2) * 10,
        recency_score = scale_0_1(as.numeric(difftime(Sys.Date(), last_commit, units = "days")) * -1 + 100),
        dev_score = experience_score * 0.4 + activity_score * 0.4 + recency_score * 0.2
      ) %>%
      arrange(desc(dev_score)) %>%
      head(5)
    
    
    dev_cards <- lapply(1:nrow(top_devs), function(i) {
      dev <- top_devs[i, ]
      tags$div(
        class = "developer-card",
        tags$div(
          class = "developer-card-header",
          tags$strong(dev$author),
          tags$span(class = "score-badge", sprintf("%.1f", dev$dev_score))
        ),
        tags$p(
          sprintf("%d коммитов за %d дней активности", 
                  dev$total_commits, dev$active_days)
        ),
        tags$small(
          sprintf("Последний коммит: %s", format(as.Date(dev$last_commit), "%d %b %Y"))
        )
      )
    })
    
    tagList(dev_cards)
  })
  
  observeEvent(input$generate_recommendations, {
    req(values$git_commit_history, values$git_diff)
    
    values$productivity_data <- analyze_dev_productivity(values$git_commit_history, values$git_diff)
    
    project_requirements <- list(
      estimated_size = input$estimated_size,
      priority = input$project_priority,
      type = input$project_type
    )
    
    values$team_recommendations <- generate_team_recommendations(
      values$git_commit_history, 
      values$git_diff, 
      project_requirements
    )
    
    output$recommendations_output <- renderUI({
      req(values$team_recommendations)
      
      team_size <- values$team_recommendations$suggested_team_size
      top_team <- values$team_recommendations$team_scoring %>%
        head(team_size)
      
      if(!"active_days" %in% names(top_team)) {
        top_team$active_days <- NA 
      }
      
      tagList(
        fluidRow(
          box(
            title = "Сводные рекомендации", width = 12, status = "primary",
            tags$div(
              class = "recommendation-item",
              tags$h4("Рекомендуемая команда"),
              tags$p(sprintf("Рекомендуемый размер команды: %d разработчиков", team_size)),
              tags$p("Ключевые разработчики:"),
              tags$ul(
                lapply(1:nrow(top_team), function(i) {
                  tags$li(
                    HTML(sprintf(
                      "<strong>%s</strong> ",
                      top_team$author[i]
                    ))
                  )
                })
              )
            )
          )
        ),
        fluidRow(
          box(
            title = "Детальная оценка разработчиков", width = 12, status = "primary",
            DTOutput("developers_rating_table")
          )
        )
      )
    })
    
    output$developers_rating_table <- renderDT({
      req(values$team_recommendations)
      
      values$team_recommendations$team_scoring %>%
        select(
          Разработчик = author,
          Опыт = experience_score,
          Активность = activity_score,
          Регулярность = regularity,
          `Основной язык` = primary_language,
          `Кол-во коммитов` = total_commits,
          `Средний размер коммита` = avg_commit_size,
          `Итоговая оценка` = total_score
        ) %>%
        datatable(options = list(
          pageLength = 10,
          scrollX = TRUE,
          language = list(url = '//cdn.datatables.net/plug-ins/1.10.25/i18n/Russian.json')
        )) %>%
        formatRound(columns = c('Опыт', 'Активность', 'Регулярность', 'Итоговая оценка'), digits = 2) %>%
        formatRound(columns = c('Средний размер коммита'), digits = 0) %>%
        formatStyle(
          'Итоговая оценка',
          background = styleColorBar(c(0, 10), 'lightblue'),
          backgroundSize = '100% 90%',
          backgroundRepeat = 'no-repeat',
          backgroundPosition = 'center'
        )
    })
  })
  
  output$collaboration_network <- renderForceNetwork({
    req(values$git_commit_history, values$git_diff)
    
    file_contributions <- values$git_diff %>%
      left_join(values$git_commit_history %>% select(commit, author, repo_id), by = c("commit"="commit", "repo_id"="repo_id")) %>%
      filter(!is.na(author)) %>%
      select(author, dst_file, repo_id) %>%
      distinct()
    
    shared_files <- file_contributions %>%
      inner_join(file_contributions, by = c("repo_id"="repo_id", "dst_file"="dst_file"), relationship = "many-to-many") %>%
      filter(author.x != author.y) %>%
      count(author.x, author.y, name = "weight") %>%
      filter(weight >= 1 )
    
    
    unique_authors <- unique(c(shared_files$author.x, shared_files$author.y))
    nodes <- data.frame(
      id = 0:(length(unique_authors) - 1),
      name = unique_authors,
      value = sapply(unique_authors, function(a) {
        sum(values$git_commit_history$author == a)
      }),
      group = 1
    )
    
    id_lookup <- setNames(nodes$id, nodes$name)
    
    edges <- shared_files %>%
      mutate(
        source = id_lookup[author.x],
        target = id_lookup[author.y]
      ) %>%
      select(source, target, value = weight)
    
    forceNetwork(
      Links = edges, Nodes = nodes,
      Source = "source", Target = "target",
      Value = "value", NodeID = "name",
      Group = "group", opacity = 0.8,
      linkWidth = JS("function(d) { return Math.sqrt(d.value); }"),
      Nodesize = "value", radiusCalculation = JS("Math.sqrt(d.nodesize)*2"),
      linkDistance = 100, charge = -30,
      fontSize = 12, zoom = TRUE, opacityNoHover = 0.5,
      colourScale = JS("d3.scaleOrdinal(d3.schemeCategory10);")
    )
  })
  
  output$project_dynamics <- renderPlotly({
    req(values$git_commit_history)
    
    commits_by_day <- values$git_commit_history %>%
      mutate(date = as.Date(substr(date, 1, 10))) %>%
      count(date, name = "commit_count")
    
    
    commits_by_day <- commits_by_day %>% 
      filter(!is.na(date))
    date_range <- seq(min(commits_by_day$date), max(commits_by_day$date), by = "day")
    complete_dates <- data.frame(date = date_range)
    
    full_data <- complete_dates %>%
      left_join(commits_by_day, by = "date") %>%
      mutate(commit_count = ifelse(is.na(commit_count), 0, commit_count))
    
    ma_data <- full_data %>%
      mutate(
        ma_7 = zoo::rollmean(commit_count, 7, fill = NA, align = "right"),
        ma_30 = zoo::rollmean(commit_count, 30, fill = NA, align = "right")
      )
    
    plot_ly() %>%
      add_trace(
        data = ma_data,
        x = ~date,
        y = ~commit_count,
        type = "scatter",
        mode = "markers",
        name = "Коммиты",
        marker = list(
          size = 4,
          color = "rgba(55, 128, 191, 0.4)",
          line = list(color = "rgba(55, 128, 191, 0.1)", width = 0.5)
        ),
        hoverinfo = "text",
        text = ~paste("Дата:", date, "<br>Коммиты:", commit_count)
      ) %>%
      add_trace(
        data = ma_data,
        x = ~date,
        y = ~ma_7,
        type = "scatter",
        mode = "lines",
        name = "7-дневное среднее",
        line = list(color = "#1f77b4", width = 2)
      ) %>%
      add_trace(
        data = ma_data,
        x = ~date,
        y = ~ma_30,
        type = "scatter",
        mode = "lines",
        name = "30-дневное среднее",
        line = list(color = "#ff7f0e", width = 2)
      ) %>%
      layout(
        title = "Динамика активности организации",
        xaxis = list(title = ""),
        yaxis = list(title = "Количество коммитов"),
        hovermode = "closest",
        legend = list(x = 0.1, y = 0.9)
      )
  })
  
  observe({
    tryCatch({
      loadData()
    }, error = function(e) {
      values$status <- "База данных пуста или не создана"
    })
  })
}

shinyApp(ui = ui, server = server)
