sustainable-fsa
<!-- README.md is generated from README.Rmd. Please edit that file -->

[![Static
Badge](https://img.shields.io/badge/Repo-sustainable--fsa%2Fusdm-magenta?style=flat)](https://github.com/sustainable-fsa/usdm-counties/)
![Last
Update](https://img.shields.io/github/last-commit/sustainable-fsa/usdm-counties?style=flat)
![Repo
Size](https://img.shields.io/github/repo-size/sustainable-fsa/usdm-counties?style=flat)

This repository provides weekly US Drought Monitor (USDM) data
aggregated to US Census county boundaries. This dataset facilitates
county-level analysis of drought conditions, supporting research,
policy-making, and climate resilience planning.

<a href="https://sustainable-fsa.github.io/usdm-counties/manifest.html" target="_blank">📂
View the US Drought Monitor county archive listing here.</a>

The goal of this repository is to aggregate county-level US Drought
Monitor data in a consistent and reproducible way, using authoritative
US Census county boundaries. Given the regulatory role played by the
interaction between USDM and county boundaries (e.g., [7 CFR
1416.205](https://www.ecfr.gov/current/title-7/section-1416.205), [7 CFR
759.5](https://www.ecfr.gov/current/title-7/section-759.5)), it is
essential that this work be authoritative, well-documented, and
available in a persistent, and findable archive established by a Federal
agency. This work seeks to create a framework for such an archive, with
the ultimate goal of conforming with the *Foundations for Evidence-Based
Policymaking Act of 2018* (“Evidence Act”, [Public Law
115–435](https://www.congress.gov/115/statute/STATUTE-132/STATUTE-132-Pg5529.pdf)),
the *Geospatial Data Act of 2018* (enacted as part of [Public Law
115–254](https://www.congress.gov/115/statute/STATUTE-132/STATUTE-132-Pg3186.pdf)),
and [Executive Order 14303, *Restoring Gold Standard
Science*](https://www.federalregister.gov/documents/2025/05/29/2025-09802/restoring-gold-standard-science).

------------------------------------------------------------------------

## 📈 About the US Drought Monitor (USDM)

The US Drought Monitor is a weekly map-based product that synthesizes
multiple drought indicators into a single national assessment. It is
produced by:

- National Drought Mitigation Center (NDMC)
- US Department of Agriculture (USDA)
- National Oceanic and Atmospheric Administration (NOAA)

Each weekly map represents a combination of data analysis and expert
interpretation.

The USDM weekly maps depicting drought conditions are categorized into
six levels:

- **None**: Normal or wet conditions
- **D0**: Abnormally Dry
- **D1**: Moderate Drought
- **D2**: Severe Drought
- **D3**: Extreme Drought
- **D4**: Exceptional Drought

While USDM drought class boundaries are developed without regard to
political boundaries, it is often aggregated by political boundaries to
assist in decision-making and for regulatory purposes. **This repository
focuses on aggregating these data to the county level, enabling more
localized analysis and decision-making.**

> **Note**: This archive is maintained by the Montana Climate Office,
> but all analytical authorship of the USDM drought maps belongs to the
> named USDM authors.

------------------------------------------------------------------------

## 🗂 Directory Structure

- `usdm-counties.R`: R script that processes and aggregates weekly USDM
  shapefiles to county boundaries.
- `data/`: Directory containing processed county-level USDM data and US
  Census county boundary files.
- `README.Rmd`: This README file, providing an overview and usage
  instructions.

------------------------------------------------------------------------

## Data Sources

- **USDM Polygons**: Weekly `.parquet` files from
  [sustainable-fsa/usdm](https://github.com/sustainable-fsa/usdm)
- **U.S. Census County Boundaries**: Downloaded from the U.S. Census
  TIGER/Line archive for 2000–2024.
- For each USDM date, the **county boundary vintage used is the most
  recently available TIGER/Line file as of that year**.
- For example:
- USDM maps from 2000 use **2000 vintage counties**
- USDM maps from 2010 use **2000 vintage counties**
- USDM maps from 2011 use **2010 vintage counties**
- USDM maps from 2014 use **2013 vintage counties**
- USDM maps from 2024 use **2023 counties** (the latest available)

All boundaries are reprojected to EPSG:4326, geometrically validated,
and saved in `.parquet` format.

## Processing Pipeline

The analysis pipeline is fully contained in
[`usdm-counties.R`](usdm-counties.R) and proceeds as follows:

1.  **Install and load dependencies**:

- Uses `pak::pak()` to ensure fresh, source-built installs of critical
  geospatial packages.

2.  **Download county boundary shapefiles** for each year (2000–2024)
    directly from US Census and store in `data/census/raw`.

3.  **Convert each shapefile to (Geo)Parquet**:

- Extract and clean up attributes (`STATEFP`, `COUNTYFP`, `NAME`,
  `NAMELSAD`)
- Encode using UTF-8, fix geometries, cast to MULTIPOLYGON, and
  calculate area
- Output is saved to `data/census/parquet/{year}-counties.parquet`

4.  **Match USDM dates with appropriate county vintage**:

- Each USDM date is paired with the most recent county boundary file
  available for that year
- The full list of USDM dates is generated starting from `2000-01-04`,
  up to two days before the current date

5.  **Download and intersect**:

- For each weekly USDM `.parquet` file:
- Read county and drought geometries
- Perform spatial intersection
- Calculate the percent of each county area affected by each drought
  class
- Tabular output is saved to `data/usdm/USDM_{YYYY-MM-DD}.parquet`

6.  **Output Structure**: Each output file is a non-spatial `.parquet`
    file with the following fields:

- `STATEFP`, `State`, `COUNTYFP`, `County`, `CountyLSAD`
- `usdm_class`: One of `None`, `D0`, `D1`, `D2`, `D3`, `D4`
- `percent`: Proportion of the county in this drought class (as a
  decimal between 0 and 1)

------------------------------------------------------------------------

## 🛠️ Dependencies

Key R packages used:

- `sf`
- `terra`
- `arrow`
- `tidyverse`
- `curl`

The script installs all required packages using the
[`pak`](https://pak.r-lib.org) package.

------------------------------------------------------------------------

## 📍 Quick Start: Visualize a Weekly County USDM Map in R

This snippet shows how to load a weekly GeoParquet file from the archive
and create a simple drought classification map using `sf` and `ggplot2`.

``` r
# Load required libraries
library(arrow)
library(sf)
library(ggplot2) # For plotting
library(tigris)  # For state boundaries
library(rmapshaper) # For innerlines function

## Get latest USDM data
latest <-
  jsonlite::fromJSON(
    "manifest.json"
  )$path |>
  stringr::str_subset("parquet") |>
  max()
# e.g., [1] "data/usdm/USDM_2025-05-27.parquet"

date <-
  latest |>
  stringr::str_extract("\\d{4}-\\d{2}-\\d{2}") |>
  lubridate::as_date()

# Get the highest (worst) drought class in each county
usdm <-
  latest |>
  arrow::read_parquet() |>
  dplyr::group_by(STATEFP, COUNTYFP) |>
  dplyr::filter(usdm_class == max(usdm_class))

counties <- 
  tigris::counties(cb = TRUE, 
                   resolution = "5m",
                   progress_bar = FALSE) |>
  dplyr::filter(
    !(STATE_NAME %in% c("Guam", 
                        "American Samoa", 
                        "United States Virgin Islands", 
                        "Commonwealth of the Northern Mariana Islands"))
  ) |>
  sf::st_cast("POLYGON", warn = FALSE, do_split = TRUE) |>
  tigris::shift_geometry()

usdm_counties <-
  usdm |>
  dplyr::left_join(counties) |>
  sf::st_as_sf()

# Plot the map
ggplot(counties) +
  geom_sf(data = sf::st_union(counties),
          fill = "grey80",
          color = NA) +
  geom_sf(data = usdm_counties,
          aes(fill = usdm_class), 
          color = NA) +
  geom_sf(data = rmapshaper::ms_innerlines(counties),
          fill = NA,
          color = "white",
          linewidth = 0.1) +
  geom_sf(data = counties |>
            dplyr::group_by(STATEFP) |>
            dplyr::summarise() |>
            rmapshaper::ms_innerlines(),
          fill = NA,
          color = "white",
          linewidth = 0.2) +
  scale_fill_manual(
    values = c("#ffff00",
               "#fcd37f",
               "#ffaa00",
               "#e60000",
               "#730000"),
    drop = FALSE,
    name = "Drought\nClass") +
  labs(title = "US Drought Monitor",
       subtitle = format(date, " %B %d, %Y")) +
  theme_void()
```

<img src="./example-1.png" style="display: block; margin: auto;" />

------------------------------------------------------------------------

## 📝 Citation & Attribution

**Citation format** (suggested):

> US Drought Monitor authors. *US Drought Monitor Weekly Maps*. Data
> curated and archived by R. Kyle Bocinsky, Montana Climate Office.
> Accessed via GitHub archive, YYYY.
> <https://sustainable-fsa.github.io/usdm/>

**Acknowledgments**:

- Map content by USDM authors.
- Data curation and archival structure by R. Kyle Bocinsky, Montana
  Climate Office, University of Montana.

------------------------------------------------------------------------

## 📄 License

- **Raw USDM data** (NDMC): Public Domain (17 USC § 105)
- **Processed data & scripts**: © R. Kyle Bocinsky, released under
  [CC0](https://creativecommons.org/publicdomain/zero/1.0/) and [MIT
  License](./LICENSE) as applicable

------------------------------------------------------------------------

## ⚠️ Disclaimer

This dataset is archived for research and educational use only. The
National Drought Mitigation Center hosts the US Drought Monitor. Please
visit <https://droughtmonitor.unl.edu>.

------------------------------------------------------------------------

## 👏 Acknowledgment

This project is part of:

**[*Enhancing Sustainable Disaster Relief in FSA
Programs*](https://www.ars.usda.gov/research/project/?accnNo=444612)**  
Supported by USDA OCE/OEEP and USDA Climate Hubs  
Prepared by the [Montana Climate Office](https://climate.umt.edu)

------------------------------------------------------------------------

## 📬 Contact

**R. Kyle Bocinsky**  
Director of Climate Extension  
Montana Climate Office  
📧 <kyle.bocinsky@umontana.edu>  
🌐 <https://climate.umt.edu>
