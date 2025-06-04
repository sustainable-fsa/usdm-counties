# update.packages(repos = "https://cran.rstudio.com/",
#                 ask = FALSE)

install.packages("pak",
                 repos = "https://cran.rstudio.com/")

# installed.packages() |>
#   rownames() |>
#   pak::pkg_install(upgrade = TRUE,
#                  ask = FALSE)

pak::pak(
  c(
    "arrow?source",
    "sf?source",
    "curl",
    "tidyverse",
    "tigris",
    "rmapshaper"
  )
)

library(magrittr)
library(tidyverse)
library(sf)
library(arrow)

sf::sf_use_s2(TRUE)

dir.create(
  file.path("data","census","raw"),
  recursive = TRUE,
  showWarnings = FALSE
)

dir.create(
  file.path("data","census","parquet"),
  recursive = TRUE,
  showWarnings = FALSE
)


states <- 
  tigris::states(cb = TRUE) %>%
  sf::st_drop_geometry() %>%
  dplyr::select(STATEFP, State = NAME) %>%
  dplyr::arrange(STATEFP)

counties <-
  c(
    `2000` = 
      "https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2000/tl_2010_us_county00.zip",
    `2009` = 
      "https://www2.census.gov/geo/tiger/TIGER2009/tl_2009_us_county.zip",
    `2010` = 
      "https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/tl_2010_us_county10.zip",
    2011:2024 %>%
      magrittr::set_names(.,.) %>%
      purrr::map_chr(
        \(x){
          paste0(
            "https://www2.census.gov/geo/tiger/TIGER",x,"/COUNTY/tl_",x,"_us_county.zip"
          )
        }
      )
  ) %>%
  {
    magrittr::set_names(
      curl::multi_download(urls = .,
                           destfiles = 
                             file.path("data","census","raw",basename(.)),
                           resume = TRUE)$destfile,
      names(.))
  } %>%
  purrr::imap_chr(\(x, year){
    outfile <- 
      file.path("data","census","parquet", paste0(year,"-counties.parquet"))
    
    if(!file.exists(outfile))
      x %>%
      file.path("/vsizip", .) %>%
      sf::read_sf() %>%
      dplyr::select(
        STATEFP = dplyr::starts_with("STATEFP"),
        COUNTYFP = dplyr::starts_with("COUNTYFP"),
        County = dplyr::starts_with("NAME") & !dplyr::starts_with("NAMELSAD"),
        `CountyLSAD` = dplyr::starts_with("NAMELSAD")
      ) %>%
      dplyr::mutate(
        County = iconv(County, from = "latin1", to = "UTF-8"),
        `CountyLSAD` = iconv(`CountyLSAD`, from = "latin1", to  = "UTF-8")
      ) %>%
      sf::st_cast("MULTIPOLYGON") %>%
      sf::st_cast("POLYGON", warn = FALSE, do_split = TRUE) %>%
      sf::st_make_valid() %T>%
      {suppressMessages(sf::sf_use_s2(FALSE))} %>%
      sf::st_make_valid() %T>%
      {suppressMessages(sf::sf_use_s2(TRUE))} %>%
      # Group by class and generate multipolygons
      dplyr::group_by(STATEFP, COUNTYFP, County, `CountyLSAD`) %>%
      dplyr::summarise(.groups = "drop",
                       is_coverage = TRUE) %>%
      sf::st_cast("MULTIPOLYGON", warn = FALSE) %>%
      sf::st_transform("EPSG:4326") %>%
      dplyr::left_join(states) %>%
      dplyr::mutate(Area = sf::st_area(geometry)) %>%
      dplyr::select(STATEFP, State, COUNTYFP, County, `CountyLSAD`, Area) %>%
      sf::write_sf(
        outfile,
        driver = "Parquet",
        layer_options = c("COMPRESSION=BROTLI",
                          "GEOMETRY_ENCODING=GEOARROW",
                          "WRITE_COVERING_BBOX=NO"),
      )
    
    return(outfile)
  }) %>%
  {
    tibble::tibble(
      Year = as.integer(names(.)) + 1, 
      Counties = .)
  } %>%
  tidyr::complete(Year = 2000:(lubridate::year(lubridate::today()))) %>%
  tidyr::fill(Counties) %>%
  tidyr::fill(Counties, .direction = "up")

usdm_get_dates <-
  function(as_of = lubridate::today()){
    as_of %<>%
      lubridate::as_date()
    
    usdm_dates <-
      seq(lubridate::as_date("20000104"), lubridate::today(), "1 week")
    
    usdm_dates <- usdm_dates[(as_of - usdm_dates) >= 2]
    
    return(usdm_dates)
  }

out <-
  usdm_get_dates() %>%
  tibble::tibble(Date = .) %>%
  dplyr::mutate(
    Year = lubridate::year(Date),
    USDM = 
      file.path("https://climate-smart-usda.github.io/usdm", 
                "usdm", "data", "parquet", 
                paste0("USDM_",Date,".parquet")),
    outfile = file.path("data", "usdm", 
                        paste0("USDM_",Date,".parquet"))
  ) %>%
  dplyr::left_join(counties) %>%
  dplyr::mutate(
    # out = furrr::future_pmap_chr(.,
    `USDM Counties` = purrr::pmap_chr(
      .l = .,
      .f = function(USDM,
                    Counties, 
                    outfile, 
                    ...){
        
        if(!file.exists(outfile))
          
          sf::st_intersection(
            Counties %>%
              sf::read_sf() %>%
              sf::`st_agr<-`("constant"),
            USDM %>%
              sf::read_sf() %>%
              sf::`st_agr<-`("constant")
          ) %>%
          sf::st_cast("MULTIPOLYGON") %>%
          sf::st_make_valid() %>%
          dplyr::arrange(STATEFP, COUNTYFP, date, usdm_class) %>%
          dplyr::mutate(
            percent = units::drop_units(sf::st_area(geometry)) / Area
          ) %>%
          sf::st_drop_geometry() %>%
          dplyr::select(STATEFP, State, COUNTYFP, County, CountyLSAD,  usdm_class, 
                        percent) %>%
          dplyr::arrange(STATEFP, COUNTYFP, usdm_class) %>%
          arrow::write_parquet(sink = outfile,
                               version = "latest",
                               compression = "zstd",
                               use_dictionary = TRUE)
        
        return(outfile)
      }
    )
  )


# outfile %>%
#   arrow::read_parquet() %>%
#   dplyr::mutate(
#     usdm_class = 
#       factor(usdm_class,
#              levels = c("None", paste0("D", 0:4)),
#              ordered = TRUE)
#   )
