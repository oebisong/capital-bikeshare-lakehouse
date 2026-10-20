# Data sources and licensing

## Sources

| Source | Location | Format |
|---|---|---|
| Trip history | `https://s3.amazonaws.com/capitalbikeshare-data/<YYYYMM>-capitalbikeshare-tripdata.zip`, listed on [capitalbikeshare.com/system-data](https://capitalbikeshare.com/system-data) | One zipped CSV per month |
| GBFS discovery | `https://gbfs.capitalbikeshare.com/gbfs/gbfs.json` | JSON |
| GBFS `station_status` | URL taken from the discovery file at run time | JSON, refreshed about every minute |
| GBFS `station_information` | URL taken from the discovery file at run time | JSON |

Both are the operator's official public interfaces. Neither needs a key.

The collector reads feed URLs from the discovery file on every run. At the
time of writing the discovery file points to feeds hosted on `gbfs.lyft.com`.
If the operator moves them, the collector follows without a code change.

### Trip file columns

Checked against the April 2025 file, which has 664,199 rows.

| Column | Example | Notes |
|---|---|---|
| `ride_id` | `75DE4C13E12A8120` | Unique per trip |
| `rideable_type` | `electric_bike` | `electric_bike` or `classic_bike` |
| `started_at`, `ended_at` | `2025-04-16 13:53:26.609` | Local Washington DC time, no offset |
| `start_station_name`, `end_station_name` | `Livingston Rd & 3rd St SE` | Empty for dockless trips |
| `start_station_id`, `end_station_id` | `31818` | Station short code. Empty for dockless trips |
| `start_lat`, `start_lng`, `end_lat`, `end_lng` | `38.95`, `-77.0` | Rounded to two decimals for dockless trips |
| `member_casual` | `member` | `member` or `casual` |

The files contain no names, email addresses or other personal details.

## Licence terms that shape the project

The data is published under the
[Capital Bikeshare Data License Agreement](https://capitalbikeshare.com/data-license-agreement),
last updated 11 January 2022. It grants a non-exclusive, royalty-free licence
to access, analyse, copy, modify and use the data for any lawful purpose. It
also lists prohibited conduct. The project handles each relevant term as follows.

| Term | How the project complies |
|---|---|
| No hosting or redistribution of the data as a stand-alone dataset | No data is committed. `.gitignore` excludes data files, and the pipeline downloads the data from the official sources. |
| Access only through the interfaces Bikeshare provides | Trip files come from the published bucket. Live data comes from the published GBFS endpoints. Nothing is scraped. |
| No data mining or extraction from the website | The project uses only the published files and feeds. |
| No attempt to correlate the data with customer names or addresses | No external data is joined. `ride_id` is masked for everyone outside the engineers group. |
| No statement or implication of affiliation or endorsement | The README states that the project is independent. |
| No use of the Capital Bikeshare name or logo as a trademark | The name appears only to identify the data source. No logo is used. |

Analyses and reports built from the data may be published for non-commercial
purposes. The dashboard and the findings in this repository fall under that.

## Licence of this repository

The code is released under the MIT licence. That licence covers the code only.
It grants no rights to the Capital Bikeshare data, which stays under the
agreement above.
