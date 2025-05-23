getGithubRepos <- function(username) {
  base_url <- paste0("https://api.github.com/users/", username, "/repos")
  req <- request(base_url) %>%
    req_url_query(per_page = 100)
  
  response <- req_perform(req)
  resp_check_status(response) 
  
  repos <- resp_body_json(response)
  sapply(repos, function(x) x$clone_url)
}


prepareRepo <- function(mode, repo_url, repo_name, clone_dir, repo_local_dir) {
  getUrlRepo <- function(repo_url, repo_name, clone_dir, mode){
    if (missing(clone_dir)) {
      clone_dir <- tempdir()
    }
    dir <- file.path(clone_dir, repo_name)
    if (file.exists(dir)) {
      git <- glue("git -C \"{dir}\" pull {repo_url}")
    } else {
      git <- glue("git clone {repo_url} \"{dir}\"")
    }
    system(git)
  }
  
  updateRepo <- function(repo_path) {
    if (file.exists(repo_path)) {
      git <- glue("git -C \"{repo_path}\" pull")
      system(git)
    }
  }
  if (mode == 0) {
    updateRepo(repo_local_dir)
  } else {
    getUrlRepo(repo_url, repo_name, clone_dir)
  }
}

getGitCommitHistory <- function(git_log_cmd, repo_id, repo_name) {
  res <- system(git_log_cmd, intern = TRUE)
  git_commit_history <- data.frame(lines=res) %>%
    tidyr::separate(
      col = .data$lines,
      into = c("commit", "parent_commit", "author", "date", "message"),
      sep = '\t'
    ) %>%
    mutate(repo_id = repo_id) %>%
    mutate(repo = repo_name)
  git_commit_history
}

getGitDiff <- function(git_diff_cmd, repo_id, repo_name) {
  diff <- system(git_diff_cmd, intern = TRUE)
  
  if (length(diff) == 0 || all(diff == "")) {
    return(data.frame(
      commit = character(),
      src_file = character(),
      dst_file = character(),
      start_del = integer(),
      count_del = integer(),
      start_add = integer(),
      count_add = integer(),
      src = character(),
      is_add = logical(),
      repo = character(),
      stringsAsFactors = FALSE
    ))
  }
  
  git_diff_df <- data.frame(lines=diff, stringsAsFactors = FALSE)
  df <- git_diff_df %>%
    filter(!grepl('^Author', lines, useBytes = TRUE)) %>%
    filter(!grepl('^Date', lines, useBytes = TRUE)) %>%
    filter(!grepl('^ ', lines, useBytes = TRUE)) %>%
    filter(!grepl('^diff', lines, useBytes = TRUE)) %>%
    filter(!grepl('^index', lines, useBytes = TRUE)) %>%
    filter(!grepl('^deleted', lines, useBytes = TRUE)) %>%
    filter(!grepl('^new', lines, useBytes = TRUE)) %>%
    filter(!grepl('^Merge', lines, useBytes = TRUE)) %>%
    filter(lines != "")
  
  res <- df %>% 
    mutate(commit_start = grepl("^commit ", lines)) %>% 
    mutate(commit_id = cumsum(commit_start)) %>% 
    mutate(commit = if_else(commit_start, stringr::str_replace(lines, "^commit ",""), NA_character_)) %>% 
    mutate(src_file = str_sub(stringr::str_extract(lines, "^\\-\\-\\-.*"), 4)) %>% 
    mutate(dst_file = str_sub(stringr::str_extract(lines, "^\\+\\+\\+.*"), 4)) %>% 
    mutate(
      range = str_match(lines, "-([0-9]+)(?:,([0-9]+))?\\s*\\+([0-9]+)(?:,([0-9]+))?"),
      start_del = as.integer(range[,2]),
      count_del = ifelse(grepl("^\\@\\@ ", lines), coalesce(as.integer(range[,3]), 1L), NA_integer_),
      start_add = as.integer(range[,4]),
      count_add = ifelse(grepl("^\\@\\@ ", lines), coalesce(as.integer(range[,5]), 1L), NA_integer_)
    ) %>% 
    select(-range) %>%
    select(lines, commit_start, commit_id, commit, src_file, dst_file, start_del, count_del, start_add, count_add) %>%
    mutate(metaline = !is.na(src_file) | !is.na(dst_file) | !is.na(commit) | !is.na(start_del))  %>% 
    mutate(segment_start = grepl("^\\@\\@ ", lines)) %>% 
    mutate(segment_id = cumsum(segment_start)) %>% 
    mutate(src_code = if_else(metaline, NA_character_, lines)) %>%
    mutate(is_add = ifelse(
      (grepl("^\\+", src_code) | grepl("^\\-", src_code)),
      grepl("^\\+", src_code),
      lag(grepl("^\\+", src_code)))) %>%
    mutate(src_code = ifelse(
      (grepl("^\\+", src_code) | grepl("^\\-", src_code)), 
      str_sub(src_code, 2), 
      src_code))
  
  src_code_concat <- res %>% 
    select(segment_id, src_code, is_add) %>% 
    filter(!is.na(src_code)) %>% 
    group_by(segment_id, is_add) %>% 
    summarise(src = paste(src_code, collapse = ""), .groups = "drop") %>%
    mutate(src = str_replace_all(src, "[^[:alnum:][:punct:]]", "")) %>%
    filter(src != "")
  
  res_fill <- res %>%
    tidyr::fill(commit, src_file, dst_file, start_del, count_del, start_add, count_add, segment_id) %>%
    filter(!is.na(src_code)) %>%
    select(commit, src_file, dst_file, start_del, count_del, start_add, count_add, segment_id)
  
  res_fill_unique <- unique(res_fill)
  
  res_src <- left_join(res_fill_unique, src_code_concat, by = "segment_id", relationship = "many-to-many") %>% 
    select(commit, src_file, dst_file, start_del, count_del, start_add, count_add, src, is_add) %>%
    mutate(repo_id = repo_id) %>%
    mutate(repo = repo_name)
  
  res_src
}

get_or_create_repo_id <- function(con, repo_name, repo_path) {
  query <- glue_sql(
    "SELECT id FROM repo_path 
     WHERE repo = {repo_name} AND path = {repo_path}",
    .con = con
  )
  
  existing_id <- dbGetQuery(con, query)
  
  if (nrow(existing_id) > 0) {
    return(existing_id$id[1])
  }
  
  max_id_query <- "SELECT COALESCE(MAX(id), 0) AS max_id FROM repo_path"
  max_id <- dbGetQuery(con, max_id_query)$max_id
  new_id <- max_id + 1
  insert_query <- glue_sql(
    "INSERT INTO repo_path (id, repo, path) 
     VALUES ({new_id}, {repo_name}, {repo_path})",
    .con = con
  )
  
  dbExecute(con, insert_query)
  new_id
}

write2db <- function(repo_name, repo_path, con) {
  repo_id <- get_or_create_repo_id(
    con = con,
    repo_name = repo_name,
    repo_path = repo_path
  )
  
  is_new <- !dbGetQuery(con, glue("SELECT EXISTS(
    SELECT 1 
    FROM git_commit_history 
    WHERE repo_id = '{repo_id}'
    ) AS has_record;"))$has_record
  if (is_new) {
    git_log_cmd <- glue("git -C \"{repo_path}\" log --format=\"%H\t%P\t%an\t%ai\t%s\" --all")
    git_diff_cmd <- glue("git -C \"{repo_path}\" log -p --unified=0 -w --ignore-blank-lines")
  } else {
    last_commit_db <- dbGetQuery(con, glue("
      SELECT commit FROM git_commit_history 
      WHERE repo_id = '{repo_id}' 
      ORDER BY date DESC LIMIT 1"))$commit
    
    first_commit_repo <- system(glue("git -C \"{repo_path}\" rev-list --max-parents=0 HEAD"), intern = TRUE)[1]
    current_head <- system(glue("git -C \"{repo_path}\" rev-parse HEAD"), intern = TRUE)
    if (last_commit_db == current_head) {
      return()
    }
    git_log_cmd <- glue("git -C \"{repo_path}\" log {last_commit_db}..HEAD --format=\"%H\t%P\t%an\t%ai\t%s\"")
    git_diff_cmd <- glue("git -C \"{repo_path}\" log -p {last_commit_db}..HEAD --unified=0 -w --ignore-blank-lines")
  }
  git_commit_history_df <- getGitCommitHistory(git_log_cmd, repo_id, repo_name)
  if (nrow(git_commit_history_df) > 0) {
    dbWriteTable(con, "git_commit_history", git_commit_history_df, append = TRUE)
  }
  
  git_diff_df <- getGitDiff(git_diff_cmd, repo_id, repo_name)
  if (nrow(git_diff_df) > 0) {
    dbWriteTable(con, "git_diff", git_diff_df, append = TRUE)
  }
}

process_local_repo <- function(repo_local_dir, con, mode) {
  repo_name <- basename(repo_local_dir)
  prepareRepo(mode, repo_local_dir = repo_local_dir)
  write2db(repo_name, repo_local_dir, con)
}

process_remote_repo <- function(repo_url, clone_dir, con, mode) {
  repo_name <- str_match(repo_url, ".*/(.+)\\.git$")[, 2]
  prepareRepo(mode, repo_url, repo_name, clone_dir)
  write2db(repo_name, file.path(clone_dir, repo_name), con)
}

processElement <- function(repo_url, clone_dir, con, mode) {
  repo_name <- str_match(repo_url, ".*/(.+)\\.git$")[, 2]
  prepareRepo(mode, repo_url, repo_name, clone_dir)
  write2db(repo_name, file.path(clone_dir, repo_name), con)
}

process_github_user <- function(username, clone_dir, con, mode) {
  repo_list <- getGithubRepos(username)
  if (length(repo_list) > 0) {
    lapply(repo_list, function(x) processElement(x, clone_dir, con, mode))
  } else {
    stop(error("repos_not_found", "Для данного пользователя не найдено репозиториев"))
  }
}

validate_dirpath <- function(path, check_exists = TRUE) {
  error <- function(class, message) {
    structure(
      list(message = message),
      class = c(class, "error", "condition")
    )
  }
  
  if (!is.character(path)) {
    stop(error("invalid_type_error", "Путь должен быть строкой"))
  }
  
  if (length(path) != 1) {
    stop(error("invalid_length_error", "Путь должен сожержать хотя бы 1 символ"))
  }
  
  if (is.na(path) || path == "") {
    stop(error("invalid_value_error", "Путь не может быть пустой строкой"))
  }
  
  normalized <- try(normalizePath(path, mustWork = FALSE, winslash = "/"), silent = TRUE)
  if (inherits(normalized, "try-error")) {
    stop(error("normalization_error", "Нормализация пути невозможна"))
  }
  
  if (.Platform$OS.type == "windows") {
    reserved <- c("CON", "PRN", "AUX", "NUL", paste0("COM",1:9), paste0("LPT",1:9))
    if (toupper(basename(normalized)) %in% toupper(reserved)) {
      stop(error("reserved_name_error", "Путь использует зарезервированные системой имена"))
    }
  }
  
  if (check_exists && !dir.exists(normalized)) {
    stop(error("not_exists_error", "Директория не существует"))
  }
  
  normalized
}

validate_git_repo <- function(path) {
  git_error <- function(class, message) {
    structure(
      list(message = message, path = path),
      class = c(class, "git_error", "error", "condition")
    )
  }
  
  tryCatch(
    {
      normalized_path <- validate_dirpath(path, check_exists = TRUE)
    },
    error = function(e) {
      stop(git_error("invalid_path_error", paste("Invalid path:", e$message)))
    }
  )
  
  git_dir <- file.path(normalized_path, ".git")
  if (!dir.exists(git_dir)) {
    stop(git_error("no_git_dir_error", "Это не Git-репозиторий (отсутствует папка .git)"))
  }
  
  tryCatch(
    {
      git_status <- system2(
        "git",
        c("-C", shQuote(normalized_path), "status"),
        stdout = NULL,
        stderr = NULL
      )
      
      if (git_status != 0) {
        stop(git_error("git_command_error", "Выполнение команды git завершилось с ошибкой"))
      }
    },
    error = function(e) {
      stop(git_error("git_system_error", paste("Ошибка проверки Git:", e$message)))
    }
  )
  
  return(normalized_path)
}

validate_git_url <- function(url, check_remote = FALSE) {
  git_url_error <- function(class, message) {
    structure(
      list(message = message, url = url),
      class = c(class, "git_url_error", "error", "condition")
    )
  }
  
  if (!is.character(url)) {
    stop(git_url_error("invalid_type", "URL должен быть строкой"))
  }
  if (length(url) != 1 || is.na(url) || url == "") {
    stop(git_url_error("invalid_value", "Некорректное значение URL"))
  }
  
  if (!grepl("\\.git$", url)) {
    stop(git_url_error("invalid_extension", "URL должен оканчиваться на .git"))
  }
  
  patterns <- list(
    https = "^https?://(.+\\.)?github\\.com/[^/]+/[^/]+\\.git$",
    ssh = "^(git@|ssh://git@)([a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,}(:[0-9]+)?(:|/)[^/]+/[^/]+\\.git$",
    scp = "^[a-zA-Z0-9_-]+@([a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,}:.+\\.git$",
    file = "^(file://)?/.+\\.git$"
  )
  
  if (!any(
    grepl(patterns$https, url, ignore.case = TRUE),
    grepl(patterns$ssh, url),
    grepl(patterns$scp, url),
    grepl(patterns$file, url)
  )) {
    stop(git_url_error("invalid_format", "Некорректный формат Git URL"))
  }
  
  if (check_remote && !grepl("^file://", url)) {
    exit_code <- system2("git", c("ls-remote", shQuote(url)), 
                         stdout = NULL, stderr = NULL)
    
    if (exit_code != 0) {
      stop(git_url_error("inaccessible", "Репозиторий недоступен"))
    }
  }
  
  return(TRUE)
}

validate_url_availability <- function(url, timeout = 10, follow_redirects = FALSE) {
  http_error <- function(class, message, status = NULL) {
    structure(
      list(message = message, url = url, status_code = status),
      class = c(class, "http_error", "error", "condition")
    )
  }
  
  if (!grepl("^https?://", url)) {
    stop(http_error("invalid_protocol", "URL должен использовать HTTP/HTTPS"))
  }
  tryCatch(
    {
      response <- GET(
        url,
        config = c(
          config(followlocation = follow_redirects),
          config(timeout = timeout)
        ),
        add_headers("User-Agent" = "R/validate-url"),
        verb = "HEAD"
      )
      
      status <- status_code(response)
      
      if (status >= 400 && status < 500) {
        stop(http_error("client_error", 
                        paste("Ошибка клиента:", status, http_status(status)$reason),
                        status))
      }
      
      if (status >= 500) {
        stop(http_error("server_error", 
                        paste("Ошибка сервера:", status, http_status(status)$reason),
                        status))
      }
      
      if (status >= 300 && status < 400 && !follow_redirects) {
        stop(http_error("redirect_error", 
                        paste("Ошибка редиректа. Код:", status),
                        status))
      }
      
      return(TRUE)
    },
    http_error = function(e) stop(e),
    error = function(e) {
      if (grepl("Timeout", e$message)) {
        stop(http_error("timeout_error", paste("Timeout:", e$message)))
      } else if (grepl("Could not resolve host", e$message)) {
        stop(http_error("dns_error", "Не удалось выполнить разрешение DNS"))
      } 
    }
  )
}

run_etl_pipeline <- function(mode, repo_url = NA, repo_local_dir = NA, 
                             clone_dir = NA, username = NA) {
  tryCatch({
    con <- dbConnect(duckdb(), dbdir = "git.duckdb")
    if (!dbExistsTable(con, "repo_path")) {
      dbExecute(con,
                "CREATE TABLE repo_path (
                  id INTEGER,
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
                  repo VARCHAR,
                  repo_id INTEGER)")
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
                  repo VARCHAR,
                  repo_id INTEGER)")
    }

    if (mode == 0) {
      validate_dirpath(repo_local_dir)
      validate_git_repo(repo_local_dir)
      process_local_repo(repo_local_dir, con, mode)
    } else if (mode == 1) {
      validate_git_url(repo_url, check_remote = TRUE)
      validate_dirpath(clone_dir)
      process_remote_repo(repo_url, clone_dir, con, mode)
    } else if (mode == 2) {
      validate_dirpath(clone_dir)
      process_github_user(username, clone_dir, con, mode)
    }
    
    dbDisconnect(con, shutdown = TRUE)
    return(list(status = "success", message = "Данные успешно загружены"))
  }, error = function(e) {
    return(list(status = "error", message = e$message))
  })
}


