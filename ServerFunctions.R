server <- function(input, output, session) {
  
  values <- reactiveValues(
    status = "",
    repo_path = NULL,
    git_commit_history = NULL,
    git_diff = NULL,
    authors = NULL,  
    selected_author = NULL,
    selected_repo = NULL,
    date_range = NULL 
  )
  
  loadData <- function() {
    con <- dbConnect(duckdb(), dbdir = "git24.duckdb")
    values$repo_path <- dbGetQuery(con, "SELECT * FROM repo_path")
    values$git_commit_history <- dbGetQuery(con, "SELECT * FROM git_commit_history LIMIT 1000")
    values$git_diff <- dbGetQuery(con, "SELECT * FROM git_diff LIMIT 1000")
    
    if(!is.null(values$git_commit_history) && nrow(values$git_commit_history) > 0) {
      values$authors <- values$git_commit_history %>%
        group_by(author) %>%
        summarise(commits = n(), 
                  first_commit = format(as.POSIXct(min(date)), "%d.%m.%Y %H:%M"), 
                  last_commit = format(as.POSIXct(max(date)), "%d.%m.%Y %H:%M"),
                  .groups = "drop")
    }
    
    dbDisconnect(con, shutdown = TRUE)
  }
  
  observeEvent(input$submit, {
    values$status <- "Запуск процесса анализа..."
    
    mode <- as.numeric(input$mode)
    
    temp_script <- tempfile(fileext = ".R")
    
    script_content <- 'library(glue) 
    library(dplyr)
    library(arrow)
    library(tidyr)
    library(stringr)
    library(DBI)
    library(duckdb)
    library(httr)
    library(jsonlite)
    
    con <- dbConnect(duckdb(), dbdir = "git24.duckdb")
    if (!dbExistsTable(con, "repo_path")) {
      dbExecute(con,
                "CREATE TABLE repo_path (
                  repo VARCHAR,
                  path VARCHAR)")
    }
    if (!dbExistsTable(con, "git_commit_history")) {
      dbExecute(con,
                "CREATE TABLE git_commit_history (
                  commit VARCHAR,
                  parent_commit VARCHAR,
                  author VARCHAR,
                  date VARCHAR,
                  message VARCHAR,
                  repo VARCHAR)")
    }
    if (!dbExistsTable(con, "git_diff")) {
      dbExecute(con,
                "CREATE TABLE git_diff (
                  commit VARCHAR,
                  src_file VARCHAR,
                  dst_file VARCHAR,
                  start_del INTEGER,
                  count_del INTEGER,
                  start_add INTEGER,
                  count_add INTEGER,
                  src VARCHAR,
                  is_add BOOLEAN,
                  repo VARCHAR)")
    }
    
    mode <- MODE_PLACEHOLDER
    
    repo_url <- "REPO_URL_PLACEHOLDER"
    repo_local_dir <- "REPO_LOCAL_DIR_PLACEHOLDER"
    clone_dir = "CLONE_DIR_PLACEHOLDER"
    username <- "USERNAME_PLACEHOLDER" 
    
    '
    
    etl_file <- readLines("ETL3.R")
    start_idx <- grep("getGithubRepos <- function", etl_file)
    end_idx <- grep("test_git_diff <- dbGetQuery", etl_file) - 1
    
    script_content <- paste0(script_content, 
                             paste(etl_file[start_idx:end_idx], collapse = "\n"),
                             "\n\ndbDisconnect(con, shutdown = TRUE)\n")
    
    script_content <- gsub("MODE_PLACEHOLDER", mode, script_content)
    
    if (mode == 0) {
      script_content <- gsub("REPO_LOCAL_DIR_PLACEHOLDER", input$repo_local_dir, script_content)
      script_content <- gsub("REPO_URL_PLACEHOLDER", "NA", script_content)
      script_content <- gsub("CLONE_DIR_PLACEHOLDER", "NA", script_content)
      script_content <- gsub("USERNAME_PLACEHOLDER", "NA", script_content)
    } else if (mode == 1) {
      script_content <- gsub("REPO_URL_PLACEHOLDER", input$repo_url, script_content)
      script_content <- gsub("CLONE_DIR_PLACEHOLDER", input$clone_dir, script_content)
      script_content <- gsub("REPO_LOCAL_DIR_PLACEHOLDER", "NA", script_content)
      script_content <- gsub("USERNAME_PLACEHOLDER", "NA", script_content)
    } else if (mode == 2) {
      script_content <- gsub("USERNAME_PLACEHOLDER", input$username, script_content)
      script_content <- gsub("CLONE_DIR_PLACEHOLDER", input$clone_dir2, script_content)
      script_content <- gsub("REPO_URL_PLACEHOLDER", "NA", script_content)
      script_content <- gsub("REPO_LOCAL_DIR_PLACEHOLDER", "NA", script_content)
    }
    
    writeLines(script_content, temp_script)
    
    withCallingHandlers(
      {
        source(temp_script)
        values$status <- paste(values$status, "\nАнализ успешно завершен!")
        loadData()
      },
      error = function(e) {
        values$status <- paste(values$status, "\nОшибка:", e$message)
      },
      warning = function(w) {
        values$status <- paste(values$status, "\nПредупреждение:", w$message)
      },
      message = function(m) {
        values$status <- paste(values$status, "\n", m$message)
      }
    )
    
    unlink(temp_script)
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
      values$selected_repo <- values$repo_path$repo[selected_row]
    }
  })
  
  # Функция для фильтрации данных по выбранному периоду
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
  
  # Функция для создания диаграммы использования языков программирования
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
      filter(!is.na(dst_file)) %>%
      distinct(dst_file) %>%
      pull(dst_file)
    
    extensions <- sapply(unique_files, function(file) {
      ext <- tolower(tools::file_ext(file))
      return(ext)
    })
    if (!is.null(extensions)){
      lang_count <- data.frame(
        ext = extensions
      ) %>%
        filter(ext %in% c("py", "r", "R", "js", "c", "cpp", "cs", "java")) %>%
        group_by(ext) %>%
        summarise(count = n()) %>%
        mutate(language = case_when(
          ext == "py" ~ "Python",
          ext %in% c("r", "R") ~ "R",
          ext == "js" ~ "JavaScript",
          ext == "c" ~ "C",
          ext == "cpp" ~ "C++",
          ext == "cs" ~ "C#",
          ext == "java" ~ "Java",
          TRUE ~ ext
        ))}
    else {
      plot(c(0, 1), c(0, 1), type = "n", axes = FALSE, xlab = "", ylab = "")
      text(0.5, 0.5, "Нет данных для отображения", cex = 1.5)
      return(NULL)
    }
    
    lang_count <- lang_count %>%
      group_by(language) %>%
      summarise(count = sum(count))
    
    if(nrow(lang_count) > 0) {
      lang_count <- lang_count %>%
        mutate(percentage = count / sum(count) * 100,
               label = paste0(language, "\n", round(percentage, 1), "%"))
      
    } else {
      return(ggplot() + 
               annotate("text", x = 0, y = 0, label = "Нет данных для отображения") +
               theme_void())
    }
  }
  
  # Функция для создания treemap репозиториев по выбранному автору
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
    

    } else {
      plot(c(0, 1), c(0, 1), type = "n", axes = FALSE, xlab = "", ylab = "")
      text(0.5, 0.5, "Нет данных для отображения", cex = 1.5)
      return(NULL)
    }
  
  # Функция для создания тепловой карты активности
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
    
  }
  
  
  # Активные часы по количеству коммитов
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
  
  # Функция для создания диаграммы количества коммитов в репозиториях
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
        group_by(repo) %>%
        summarise(commits = n(), .groups = "drop") %>%
        arrange(desc(commits))
      
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
    
    
    output$author_repo_count <- renderText({
      author_repos <- values$git_commit_history %>%
        filter(author == values$selected_author) %>%
        distinct(repo) %>%
        nrow()
      return(as.character(author_repos))
    })
    
    author_date_range <- reactive({
      input$author_date_range
    })
    
    # Создаем диаграмму использования языков программирования для автора
    output$author_lang_chart <- renderPlot({
      req(values$git_diff)
      
      author_commits <- values$git_commit_history %>%
        filter(author == values$selected_author) %>%
        pull(commit)
      
      git_diff_filtered <- values$git_diff %>%
        filter(commit %in% author_commits)
      
      createLanguageUsageChart(git_diff_filtered, author_date_range())
    })
    
    # Создаем treemap для автора
    output$author_treemap <- renderPlot({
      req(values$git_diff)
      
      author_commits <- values$git_commit_history %>%
        filter(author == values$selected_author) %>%
        pull(commit)
      
      git_diff_filtered <- values$git_diff %>%
        filter(commit %in% author_commits)
      
      createRepoTreemap(git_diff_filtered, author_date_range())
    })
    
    # Создаем тепловую карту активности для автора
    output$author_heatmap <- renderPlotly({
      req(values$git_commit_history)
      
      commit_data_filtered <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      createActivityHeatmap(commit_data_filtered, author_date_range())
    })
    
    # Создаем столбчатую диаграмму коммитов по репозиториям для автора
    output$author_commits_chart <- renderPlotly({
      req(values$git_commit_history)
      
      commit_data_filtered <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      createCommitsByRepoChart(commit_data_filtered, author_date_range())
    })
    
    # Создаем диаграмму активных часов для автора
    output$author_active_hours <- renderPlotly({
      req(values$git_commit_history)
      
      commit_data_filtered <- values$git_commit_history %>%
        filter(author == values$selected_author)
      
      createActiveHoursChart(commit_data_filtered, author_date_range())
    })
  })
  
  # Анализ выбранного репозитория
  observeEvent(input$analyze_repo, {
    req(values$selected_repo)
    
    repo_commits_data <- values$git_commit_history %>% 
      filter(repo == values$selected_repo)
    
    repo_diff_data <- values$git_diff %>% 
      filter(repo == values$selected_repo)
    
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
    
    # Диаграмма вклада участников
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
      
    })
    
    # Диаграмма типов изменений
    output$repo_changes_chart <- renderPlotly({
      data <- filtered_data()
      
      changes_data <- data$diff %>%
        group_by(author = data$commits$author[match(commit, data$commits$commit)]) %>%
        summarise(
          add = sum(count_add, na.rm = TRUE),
          del = sum(count_del, na.rm = TRUE)
        )
    })
    
    # График истории участия
    output$repo_history_chart <- renderPlotly({
      data <- filtered_data()
      
      history_data <- data$commits %>%
        mutate(date = as.Date(substr(date, 1, 10))) %>%
        group_by(date, author) %>%
        summarise(count = n(), .groups = "drop")
    })
    
    #Диаграмма файлов
    output$repo_file_tree <- renderPlot({
      data <- filtered_data()
      
      file_data <- data$diff %>%
        group_by(dst_file) %>%
        summarise(commits = n_distinct(commit)) %>% 
        filter(!is.na(dst_file))
    })
  })

}

shinyApp(ui = ui, server = server)