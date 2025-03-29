library(glue) # Для вызова системных команд
library(dplyr)
library(arrow)

repo_url <- "https://github.com/tidyverse/ggplot2.git"

# Клонирует репозиторий во временную или в указанную директорию
# Если директория не пуста, обновляет информацию про репозиторий
getRepo <- function(repo_url, dir_parent){
  if (missing(dir_parent)) {
    dir_parent <- tempdir()
  }
  dir <- strsplit(repo_url, "//")[[1]][2]
  dir <- file.path(dir_parent, dir)
  if (file.exists(dir)) {
    git <- glue("git -C {dir} pull {repo_url}")
  } else {
    git <- glue("git clone {repo_url} {dir}")
  }
  system(git)
  return(dir)
}

repo_dir <- getRepo(repo_url)

git_log_cmd <- glue('git -C {repo_dir} log --format="%H\t%P\t%an\t%ai\t%s" --all')
res <- system(git_log_cmd, intern = TRUE)

git_commit_history <- data.frame(lines=res) %>%
  tidyr::separate(
    col = .data$lines,
    into = c("commitHash", "parentHash", "author", "date", "message"),
    sep = '\t'
    )

write_parquet(git_commit_history, paste(gsub("/", "_", strsplit(repo_url, "//")[[1]][2]),".parquet", sep=""))

git_diff_cmd <- glue('git -C {repo_dir} log -p --unified=0 -w --ignore-blank-lines')
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
  filter(lines != "")

i = 1
n = nrow(df)
changesDf <- data.frame(commitHash= numeric(0), fileA= numeric(0), fileB= numeric(0), fromFileRange= numeric(0), toFileRange= numeric(0), isAdd= numeric(0), text= numeric(0))
j = 1
while (i < 50000) {
  commit = NA
  fileA = NA
  fileB = NA
  fromFileRange = NA
  toFileRange = NA
  isAdd = NA
  text = NA
  if (grepl("^commit", df$lines[i])) {
    commit <- strsplit(df$lines[i], " ")[[1]][2]
    i = i + 1
  }
  while (!grepl("^commit", df$lines[i])) {
    if (grepl("^---", df$lines[i])) {
      fileA <- substring(strsplit(df$lines[i], " ")[[1]][2], 3)
      i = i + 1
    }
    if (grepl("^\\+\\+\\+", df$lines[i])) {
      fileB <- substring(strsplit(df$lines[i], " ")[[1]][2], 3)
      i = i + 1
    }
    if (grepl("^@@", df$lines[i])) {
      fromFileRange <- strsplit(df$lines[i], " ")[[1]][2]
      toFileRange <- strsplit(df$lines[i], " ")[[1]][3]
      i = i + 1
    }
    while ((grepl("^-", df$lines[i]) | grepl("^\\+", df$lines[i])) & !(grepl("^---", df$lines[i]) | grepl("^\\+\\+\\+", df$lines[i]))) {
      isAdd = grepl("^\\+", df$lines[i])
      text = substring(df$lines[i], 2)
      changesDf[j, ] <- c(commit, fileA, fileB, fromFileRange, toFileRange, isAdd, text)
      i = i + 1
      j = j + 1
    }
    if (!(grepl("^commit", df$lines[i]) | grepl("^---", df$lines[i]) | grepl("^\\+\\+\\+", df$lines[i]) | grepl("^@@", df$lines[i]))) {
      i = i + 1
    }
  }
}

write_parquet(changesDf, paste(gsub("/", "_", strsplit(repo_url, "//")[[1]][2]),"_changes.parquet", sep=""))
test <- read_parquet(paste(gsub("/", "_", strsplit(repo_url, "//")[[1]][2]),".parquet", sep=""))
