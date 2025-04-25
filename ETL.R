library(glue) 
library(dplyr)
library(arrow)
library(tidyr)
library(stringr)
library(DBI)
library(duckdb)
library(httr)
library(jsonlite)

# Подключение к базе данных
con <- dbConnect(duckdb(), dbdir = "git5.duckdb")
if (!dbExistsTable(con, 'repo_path')) {
  dbExecute(con,
            "CREATE TABLE repo_path (
              repo VARCHAR,
              path VARCHAR)")
}

# Режим
# 0 - локальный репозиторий
# 1 - удаленный репозиторий
# 2 - имя пользователя на github
mode <- 2

# Входные данные
repo_url <- "https://github. com/tidyverse/ggplot2.git"
repo_local_dir <- "D:/Творчество/git-analysis/ggplot2"
clone_dir = "D:/Творчество/git-analysis/MariUt005"
username <- "MariUt005" 
token <- NA


getGithubRepos <- function(username, token = NULL) {
  base_url <- paste0("https://api.github.com/users/", username, "/repos")
  query <- list(
    type = "all", 
    per_page = 100  
  )
  headers <- if (!is.null(token)) {
    add_headers(Authorization = paste("token", token))
  } else {
    add_headers()
  }
  all_repos <- list()
  page <- 1
  repeat {
    query$page <- page
    response <- GET(url = base_url, query = query, headers)
    if (http_status(response)$category != "Success") {
      stop("Ошибка: ", http_status(response)$message)
    }
    repos <- content(response, "parsed")
    if (length(repos) == 0) break
    all_repos <- c(all_repos, repos)
    links_header <- headers(response)$link
    if (is.null(links_header) || !grepl('rel="next"', links_header)) {
      break
    }
    page <- page + 1
  }
  repo_links <- sapply(all_repos, function(x) x$clone_url)
  return(repo_links)
}


prepareRepo <- function(mode, repo_url, repo_name, clone_dir, repo_local_dir) {
  getUrlRepo <- function(repo_url, repo_name, clone_dir){
    if (missing(clone_dir)) {
      clone_dir <- tempdir()
    }
    dir <- file.path(clone_dir, repo_name)
    if (file.exists(dir)) {
      git <- glue("git -C {dir} pull {repo_url}")
    } else {
      git <- glue("git clone {repo_url} {dir}")
    }
    system(git)
  }
  
  # Обновляет репозиторий
  updateRepo <- function(repo_path) {
    if (file.exists(repo_path)) {
      git <- glue("git -C {repo_path} pull")
      system(git)
    }
  }
  if (mode == 0) {
    updateRepo(repo_local_dir)
  } else {
    getUrlRepo(repo_url, repo_name, clone_dir)
  }
}

# Получить git_commit_history
getGitCommitHistory <- function(git_log_cmd, repo_name) {
  #repo_name <- "git-analysis"
  #git_log_cmd <- glue('git -C D:/Творчество/git-analysis/MariUt005/git-analysis log --format="%H\t%P\t%an\t%ai\t%s" --all')
  
  res <- system(git_log_cmd, intern = TRUE)
  git_commit_history <- data.frame(lines=res) %>%
    tidyr::separate(
      col = .data$lines,
      into = c("commit", "parent_commit", "author", "date", "message"),
      sep = '\t'
    ) %>%
    mutate(repo = repo_name)
  git_commit_history
}


getGitDiff <- function(git_diff_cmd, repo_name) {
  repo_name <- "git-analysis"
  git_diff_cmd <- glue('git -C D:/Творчество/git-analysis/MariUt005/git-analysis log -p --unified=0 -w --ignore-blank-lines')
  print(git_diff_cmd, repo_name)
  
  
  diff <- system(git_diff_cmd, intern = TRUE)
  
  git_diff_df <- data.frame(lines=diff)
  df <- git_diff_df %>%
    filter(!grepl('^Author', lines)) %>%
    filter(!grepl('^Date', lines)) %>%
    filter(!grepl('^ ', lines)) %>%
    filter(!grepl('^diff', lines)) %>%
    filter(!grepl('^index', lines)) %>%
    filter(!grepl('^deleted', lines)) %>%
    filter(!grepl('^new', lines)) %>%
    filter(!grepl('^Merge', lines)) %>%
    filter(lines != "")
  
  res <- df %>% 
    mutate(commit_start = grepl("^commit ", lines)) %>% 
    mutate(commit_id = cumsum(commit_start)) %>% 
    mutate(commit = if_else(commit_start, stringr::str_replace(lines, "^commit ",""), NA)) %>% 
    mutate(src_file = str_sub(stringr::str_extract(lines, "^\\-\\-\\-.*"), 4)) %>% 
    mutate(dst_file = str_sub(stringr::str_extract(lines, "^\\+\\+\\+.*"), 4)) %>% 
    mutate(
      range = str_match(lines, "-([0-9]+)(?:,([0-9]+))?\\s*\\+([0-9]+)(?:,([0-9]+))?")[,2:5],
      start_del = as.integer(range[,1]),
      count_del = ifelse(grepl("^\\@\\@ ", lines), coalesce(as.integer(range[,2]), 1), NA),
      start_add = as.integer(range[,3]),
      count_add = ifelse(grepl("^\\@\\@ ", lines), coalesce(as.integer(range[,4]), 1), NA)
    ) %>% 
    select(lines, commit_start, commit_id, commit, src_file, dst_file, start_del, count_del, start_add, count_add) %>%
    mutate(metaline = !is.na(src_file) | !is.na(dst_file) | !is.na(commit) | !is.na(start_del))  %>% 
    mutate(segment_start = grepl("^\\@\\@ ", lines)) %>% 
    mutate(segment_id = cumsum(segment_start)) %>% 
    mutate(src_code = if_else(metaline, NA, lines)) %>%
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
    summarise(src = paste(src_code, collapse = "")) %>%
    mutate(src = str_replace_all(src, "[^[:alnum:][:punct:]]", "")) %>%
    filter(src != "")
  
  res_fill <- res %>%
    tidyr::fill(commit, src_file, dst_file, start_del, count_del, start_add, count_add, segment_id) %>%
    filter(!is.na(src_code)) %>%
    select(commit, src_file, dst_file, start_del, count_del, start_add, count_add, segment_id)
  
  res_fill_unique <- unique(res_fill)
  
  res_src <- left_join(res_fill_unique,src_code_concat, by = "segment_id") %>% 
    select(commit, src_file, dst_file, start_del, count_del, start_add, count_add, src, is_add) %>%
    mutate(repo = repo_name)
  res_src
}

write2db <- function(repo_name, repo_path) {
  if (!dbExistsTable(con, 'git_commit_history')) {
    is_new <- TRUE
  } else {
    is_new <- !dbGetQuery(con, glue("SELECT EXISTS(
      SELECT 1 
      FROM git_commit_history 
      WHERE repo = '{repo_name}'
      ) AS has_record;"))$has_record
  }
  if (is_new) {
    git_log_cmd <- glue('git -C {repo_path} log --format="%H\t%P\t%an\t%ai\t%s" --all')
    git_diff_cmd <- glue('git -C {repo_path} log -p --unified=0 -w --ignore-blank-lines')
  } else {
    last_commit <- dbGetQuery(con, glue("SELECT commit
      FROM (
        SELECT 
          commit,
          ROW_NUMBER() OVER (ORDER BY CAST(SUBSTRING(date, 1, 19) AS TIMESTAMP) DESC) AS rn
        FROM git_commit_history
        WHERE repo = '{repo_name}'
      ) AS ranked
      WHERE rn = 1;"))$commit
    git_log_cmd <- glue('git -C {repo_path} log {last_commit}..HEAD --format="%H\t%P\t%an\t%ai\t%s"')
    git_diff_cmd <- glue('git -C {repo_path} log -p {last_commit}..HEAD --unified=0 -w --ignore-blank-lines')
  }
  git_commit_history_df <- getGitCommitHistory(git_log_cmd, repo_name)
  dbWriteTable(con, "git_commit_history", git_commit_history_df, append = TRUE)
  git_diff_df <- getGitDiff(git_diff_cmd, repo_name)
  dbWriteTable(con, "git_diff", git_diff_df, append = TRUE)
  if (is_new) {
    repo_path_df <- data.frame(
      repo = repo_name,
      path = repo_path
    )
    dbWriteTable(con, "repo_path", repo_path_df, append = TRUE)
  }
}

processElement <- function(repo_url, clone_dir) {
  repo_name <- str_match(repo_url, ".*/(.+)\\.git$")[, 2]
  prepareRepo(mode, repo_url, repo_name, clone_dir)
  print(file.path(clone_dir, repo_name))
  write2db(repo_name, file.path(clone_dir, repo_name))
}

if (mode == 0) {
  repo_name <- basename(repo_local_dir)
  prepareRepo(mode, repo_local_dir = repo_local_dir)
  write2db(repo_name, repo_local_dir)
} else if (mode == 1) {
  repo_name <- str_match(repo_url, ".*/(.+)\\.git$")[, 2]
  prepareRepo(mode, repo_url, repo_name, clone_dir)
  write2db(repo_name, file.path(clone_dir, repo_name))
} else if (mode == 2) {
  repo_list <- getGithubRepos(username)
  print(repo_git_list)
  lapply(repo_git_list, function(x) processElement(x, clone_dir))
}

test_repo_path <- dbGetQuery(con, "SELECT * FROM repo_path;")
test_git_commit_history <- dbGetQuery(con, "SELECT * FROM git_commit_history;")
test_git_diff <- dbGetQuery(con, "SELECT * FROM git_diff;")


dbDisconnect(con, shutdown = TRUE)
