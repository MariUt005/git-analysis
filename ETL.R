library(glue) # Для вызова системных команд
library(dplyr)

# Клонирует репозиторий во временную или в указанную директорию
# Если директория не пуста, обновляет информацию про репозиторий
getRepo <- function(repo_url, dir_parent){
  if (missing(dir_parent)) {
    dir_parent <- tempdir()
  }
  dir <- strsplit(repo_url, "//")[[1]][2]
  #gsub("/", "\\\\", dir)
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

dir_repo <- getRepo("https://github.com/tidyverse/ggplot2.git")
#list.files(dir_repo)
#print(dir_repo)

