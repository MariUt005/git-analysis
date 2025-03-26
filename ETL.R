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
  print(dir)
  if (file.exists(dir)) {
    git <- glue("git -C {dir} pull {repo_url}")
    print("pull")
  } else {
    git <- glue("git clone {repo_url} {dir}")
    print("clone")
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
test <- read_parquet(paste(gsub("/", "_", strsplit(repo_url, "//")[[1]][2]),".parquet", sep=""))
