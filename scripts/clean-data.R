library(magrittr)

SHEET_ID <- "Add your sheet ID here"

#' Run script
main <- function () {
  old_data <-
    read_kanshiin_wx_data(Sys.glob(here::here("raw-data/-2017/*.xlsx")), read_kanshiin_wx_file)
  
  new_data <- read_kanshiin_wx_data(Sys.glob(here::here("raw-data/2017-2020/*.xlsx")), read_new_kanshiin_wx_file)
  
  df <- dplyr::bind_rows(old_data, new_data)
  
  d <- df %>%
    dplyr::mutate(year = lubridate::year(timestamp)) %>%
    dplyr::nest_by(year) %>%
    dplyr::mutate(filename = here::here(glue::glue("data/{year}.csv"))) %>% 
    dplyr::mutate(year = as.character(year))
  
  # purrr::walk2(d$filename, d$data, ~ readr::write_csv(.y, .x))
  purrr::walk2(d$data, d$year, ~ write_to_sheet(.x, .y))
}

write_to_sheet <- function (df, year) {
  googlesheets4::sheet_write(df, SHEET_ID, year)
}

#' Read weather data from Kanshiin excel records
#'
#' @param filenames - A character vector of filenames to load.
#' @param years - The dates in the file are often wrong so the years should be set manually. Can be either a numeric vector or function that takes the current timestamp and filename, returning a new timestamp.
#' @param include_extra - Include the daily temperature average and notes
#'
#' @return - Tibble containing all the records
#'
#' @examples
#' read_kanshiin_wx_data(c("data/2004.xlsx", "data/2017.xlsx"), years = c(2004, 2017))
read_kanshiin_wx_data <- function (filenames, loader) {
  df <- tibble::tibble(filename = filenames) %>%
    dplyr::mutate(data = purrr::map(filename, ~ loader(.x))) %>%
    tidyr::unnest(data)
  
  return(
    dplyr::select(df, -filename) %>%
      dplyr::arrange(timestamp) %>%
      dplyr::mutate(
        year = lubridate::year(timestamp),
        date = lubridate::date(timestamp),
        hour = lubridate::hour(timestamp)
      ) %>%
      dplyr::select(
        timestamp,
        year,
        date,
        hour,
        score,
        temp,
        snow,
        wind,
        weather,
        visibility,
        notes
      )
  )
}

read_kanshiin_wx_file <- function (filename) {
  raw_df <- readxl::read_excel(filename)
  
  df <- raw_df[, 1:7] # Ignore extra columns
  times <- colnames(df)[3:5] %>%
    as.numeric()
  
  if (times[1] < 1) {
    times <- times * 24 # Excel's time format is weird
  }
  
  times <- format(times) # Convert to string
  
  colnames(df) <- c("date", "komoku", times, "avg_temp", "notes")
  
  df$snow <- NA_character_
  
  file_year <- stringr::str_extract(filename, "20\\d{2}\\.") %>%
    as.numeric()
  
  df <- df %>%
    dplyr::mutate(
      date = fillr::fill_missing_previous(date),
      date = dplyr::case_when(
        is.numeric(date) ~ lubridate::as_date(date),
        is.character(date) ~ lubridate::as_date(as.numeric(date)),
        TRUE ~ lubridate::date(date)
      ),
      date = update(date, year = file_year)
    )
  
  data <- df %>%
    dplyr::select(-c(avg_temp, notes)) %>%
    tidyr::pivot_longer(tidyselect::all_of(times),
                        names_to = "time",
                        values_to = "value") %>%
    dplyr::mutate(time = as.numeric(time)) %>%
    dplyr::mutate(timestamp = update(date, hour = floor(time), minutes = (time %% 1) * 60)) %>%
    tidyr::pivot_wider(names_from = komoku, values_from = value) %>%
    dplyr::select(-c(date, time)) %>%
    dplyr::rename(temp = "気温",
                  weather = "天候",
                  summit_visible = "山頂") %>%
    dplyr::mutate(
      summit_visible = as.numeric(summit_visible),
      summit_visible = dplyr::case_when(summit_visible == 1 ~ TRUE, !is.na(weather) ~ FALSE,
                                        TRUE ~ NA)
    ) %>%
    dplyr::mutate(temp = stringr::str_remove(temp, "^[^-\\d]"),
                  # Remove anything from start this is not - or number
                  temp = as.numeric(temp)) %>%
    dplyr::mutate(weather = factor(weather)) %>%
    dplyr::mutate(visibility = ifelse(summit_visible, 10, NA)) %>%
    dplyr::mutate(score = NA,
                  wind = NA,
    ) %>% 
    dplyr::mutate(
      date = lubridate::date(timestamp)
    )
  
  notes <- df %>%
    dplyr::select(date, notes) %>%
    dplyr::filter(!is.na(notes))
  
  return(
    data %>%
      dplyr::left_join(notes, by = "date") %>%
      dplyr::arrange(timestamp) %>%
      dplyr::mutate(
        year = lubridate::year(timestamp),
        date = lubridate::date(timestamp),
        hour = lubridate::hour(timestamp)
      ) %>%
      dplyr::select(
        timestamp,
        date,
        hour,
        score,
        temp,
        snow,
        wind,
        weather,
        visibility,
        notes
      )
  )
}

read_new_kanshiin_wx_file <- function (filename) {
  raw_df <- readxl::read_excel(filename)
  
  df <- raw_df[, 1:8] # Ignore extra columns
  times <- colnames(df)[3:5] %>%
    as.numeric()
  
  if (times[1] < 1) {
    times <- times * 24 # Excel's time format is weird
  }
  
  times <- format(times) # Convert to string
  
  colnames(df) <- c("date", "komoku", times, "avg_temp", "snow", "notes")
  
  file_year <- stringr::str_extract(filename, "20\\d{2}\\.") %>%
    as.numeric()
  
  df$date <- fillr::fill_missing_previous(df$date)
  
  if (is.numeric(df$date)) {
    df$date <- lubridate::as_date(df$date)
  } else if (is.character(date)) {
    df$date <- lubridate::as_date(as.numeric(df$date))
  } else {
    df$date <- lubridate::date(df$date)
  }
  
  df$date <- update(df$date, year = file_year)
  
  data <- df %>%
    dplyr::select(-c(avg_temp, notes, snow)) %>%
    tidyr::pivot_longer(tidyselect::all_of(times),
                        names_to = "time",
                        values_to = "value") %>%
    dplyr::mutate(time = as.numeric(time)) %>%
    dplyr::mutate(timestamp = update(date, hour = floor(time), minutes = (time %% 1) * 60)) %>%
    tidyr::pivot_wider(names_from = komoku, values_from = value) %>%
    dplyr::select(-c(date, time)) %>%
    dplyr::rename(temp = "気温",
                  weather = "天候",
                  summit_visible = "山頂") %>%
    dplyr::mutate(
      summit_visible = as.numeric(summit_visible),
      summit_visible = dplyr::case_when(summit_visible == 1 ~ TRUE, !is.na(weather) ~ FALSE,
                                        TRUE ~ NA)
    ) %>%
    dplyr::mutate(temp = stringr::str_remove(temp, "^[^-\\d]"),
                  # Remove anything from start this is not - or number
                  temp = as.numeric(temp)) %>%
    dplyr::mutate(weather = factor(weather)) %>%
    dplyr::mutate(visibility = ifelse(summit_visible, 10, NA)) %>%
    dplyr::mutate(score = NA,
                  wind = NA,
    ) %>% 
    dplyr::mutate(
      date = lubridate::date(timestamp)
    )
  
  notes <- df %>%
    dplyr::select(date, notes) %>% 
    dplyr::filter(!is.na(notes))
  
  snow <- df %>%
    dplyr::select(date, snow) %>% 
    dplyr::filter(!is.na(snow))
  
  return(
    data %>%
      dplyr::left_join(notes, by = "date") %>%
      dplyr::left_join(snow, by = "date") %>%
      dplyr::arrange(timestamp) %>%
      dplyr::mutate(
        year = lubridate::year(timestamp),
        date = lubridate::date(timestamp),
        hour = lubridate::hour(timestamp)
      ) %>%
      dplyr::select(
        timestamp,
        date,
        hour,
        score,
        temp,
        snow,
        wind,
        weather,
        visibility,
        notes
      )
  )
}

#' A helper to update a timestamps year based on a filename
#'
#' @param timestamp
#' @param filename
#'
#' @return NA if no match
#'
#' @examples
#' year_from_filename(lubridate::now(), "2004.xlsx")
year_from_filename <- function (timestamp, filename) {
  year <- stringr::str_extract(filename, "20\\d{2}\\.") %>%
    as.numeric()
  
  return(as.numeric(update(timestamp, year = year)))
}

# main()