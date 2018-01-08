library(MonetDBLite)
library(tidyverse)
library(stringr)
library(DBI)

url_base <- "http://212.47.238.202/geo_sirene/last/geo-sirene_"

tmp <- tempdir()


conn <- src_monetdblite("~/monetdb")

for (i in c(str_pad(c(1:19, 21:95), width = 2, side = "left", pad = "0"), "2A", "2B")) {
  httr::GET(paste0(url_base, i, ".csv.7z"), httr::write_disk(paste0(tmp, "/geo-sirene_", i, ".csv.7z"), overwrite = TRUE))
  system(paste0('7z e -o', tmp, " ", tmp, "/geo-sirene_", i, ".csv.7z"))
  tmp_csv <- read_csv(paste0(tmp, "/geo-sirene_", i, ".csv"), na = c("NR", "NN"), col_types = cols(.default = col_character(),
    longitude = col_double(),
    latitude = col_double(),
    geo_score = col_double(),
    geo_type = col_character(),
    geo_adresse = col_character(),
    geo_id = col_character(),
    geo_ligne = col_character()
  ))
  sirene <- dbWriteTable(conn$con, "sirene", tmp_csv, append = TRUE)
  rm(tmp_csv)
  gc()

}
