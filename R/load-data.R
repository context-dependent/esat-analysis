library(tidyverse)


latest_cache_file <- function(obj_id) {
  cache_dir <- file.path(Sys.getenv("Z_HOME"), "Data", "cache", obj_id)

  if(!fs::dir_exists(cache_dir)) {
    return(NULL)
  }

  cache_files <- cache_dir |> 
    fs::dir_ls() |> 
    sort()

  if (length(cache_files) == 0) {
    return(NULL)
  }
  
  dplyr::last(cache_files)
} 

new_cache_file <- function(obj_id, extension = ".rds") {
  obj_dir <- file.path(Sys.getenv("Z_HOME"), "Data", "cache", obj_id)
  if(!fs::dir_exists(obj_dir)) {
    fs::dir_create(obj_dir)
  }
  
  file.path(
    obj_dir, 
    glue::glue("{strftime(lubridate::now(), format = '%Y%m%d%H%M%S')}_{extension}")
  )
}

survey_list <- function(fetch = FALSE) {
  file <- here::here("data/survey_list.rds")
  if(fetch || !fs::file_exists(file)) {
    readr::write_rds(bpqx::list_surveys(), file)
  }
  readr::read_rds(file) |> 
    dplyr::select(qx_survey_id = id, survey_name = name)
}

save_survey <- function(qx_survey_id) {
  d <- bpqx::fetch_responses(qx_survey_id)
  cache_file <- new_cache_file(qx_survey_id)
  readr::write_rds(d, cache_file)
}

load_sf <- function(fetch = FALSE) {

  readr::read_csv(latest_cache_file("sf"), show_col_types = FALSE) |> 
    dplyr::select(-dplyr::where(~all(is.na(.x)))) |> 
    dplyr::mutate(
      pas_id = stringr::str_sub(pas_id, 1, 15),
      survey_completed = !is.na(response_id)
    ) 
}

load_survey <- function(qx_survey_id, fetch = FALSE) {
  if (fetch || is.null(latest_cache_file(qx_survey_id))) {
    save_survey(qx_survey_id)
  } 
  readr::read_rds(latest_cache_file(qx_survey_id))
}


load_surveys <- function(sf, fetch = FALSE) {
  sf |> 
    dplyr::group_by(qx_survey_id) |> 
    dplyr::group_nest(.key = "sf") |> 
    dplyr::mutate(qx = purrr::map(qx_survey_id, ~ load_survey(.x, fetch = fetch))) |> 
    dplyr::left_join(survey_list(), by = "qx_survey_id") |> 
    dplyr::mutate(
      survey_time = survey_name |> 
        stringr::str_extract(r"((?<=_s)\d{2})") |> 
        readr::parse_integer(), 
      survey_tag = survey_name |> 
        stringr::str_extract(r"((?<=s\d{2}_).*$)") 
    ) |> 
    dplyr::arrange(survey_time) |> 
    dplyr::mutate(
      survey_tag = fct_inorder(survey_tag)
    ) |> 
    dplyr::select(-survey_name)
}

join_surveys <- function(d, by = c("response_id")) {
  d |>  
    dplyr::mutate(
      data = purrr::map2(
        sf, qx, ~dplyr::left_join(
          .x, janitor::clean_names(.y), 
          by = by, 
          suffix = c("", "_qx")
        )
      )
    ) |> 
    dplyr::select(qx_survey_id, data, matches("^survey_"))
}


`%fill%` <- function(x, y) {
  if (is.na(x)) {
    y
  } else {
    x
  }
}

first_non_missing <- function(x, a = NA) {
  if (length(x) == 0 || !is.na(a)) {
    return(a)
  } else {
    return(first_non_missing(x[-1], x[1]))
  }
}

load_data <- memoise::memoise(function(fetch = FALSE, join_by = "response_id") {
  readRenviron(here::here(".env"))
  load_sf(fetch = fetch) |> 
    load_surveys(fetch = fetch) |> 
    join_surveys(join_by)
})
