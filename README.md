# Анализ Git репозиториев

Состав команды: Арикова Кристина Георгиевна, Гусарова Александра Павловна, Утенкова Мария Александровна

Капитан команды: Утенкова Мария Александровна

Руководитель: [i2z1](https://github.com/i2z1)

## Как запустить?

### Требования:

1. [R версии 4.0 и выше](https://www.r-project.org/)
2. [Git](https://git-scm.com/) (проект тестировался на версии git 2.48 под Windows)
3. [RStudio](https://posit.co/download/rstudio-desktop/) (по желанию)

### Установка пакетов

Для корректной работы ПО необходимо установить [пакеты](), для чего можете выполнить команду:

```
install.packages(c(
  "shiny", "shinydashboard", "DT", "dplyr", "plotly", "tidyr", 
  "ggplot2", "RColorBrewer", "treemap", "glue", "httr2", "DBI", 
  "duckdb", "stringr", "markdown", "shinyjs", "networkD3", 
  "zoo", "igraph", "lubridate"
))
```

### Запуск

Для запуска проекта вы можете выбрать любой удобный способ:
- Запуск через консоль R:
```
shiny::runApp('git-analysisProject.R')
```
- Запуск через RStudio:
  1. Откройте репозиторий в RStudio
  2. Откройте файл git-analysisProject.R
  3. Нажмите на кнопку Run App в правом верхнем углу панели с кодом
     
     ![image](https://github.com/user-attachments/assets/6abaa0d5-5da4-40c4-a6cb-08f310982848)


