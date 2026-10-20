# Engineering notes

Problems found in the real data or during the build, and how each was handled.
Each note follows the same order: problem, impact, solution, validation and
trade-off.

## Stray file inside each trip archive

**Problem.** Every monthly zip holds the trip CSV and a second entry,
`__MACOSX/._<name>.csv`, left by the macOS archiver. That entry is binary
metadata with a `.csv` name.

**Impact.** Extracting everything would put a binary file in the landing
folder. Auto Loader would read it as CSV and load garbage rows into bronze.

**Solution.** The downloader extracts only the CSV entry outside `__MACOSX/`
and fails if it finds anything other than exactly one such file.

**Validation.** `tests/unit/test_collectors.py` builds an archive with the
same layout and checks that only the real CSV is selected.

**Trade-off.** If the operator ever publishes two CSV files in one archive,
the download fails loudly. That is preferable to guessing.

## A quarter of trips have no station

**Problem.** In April 2025, 180,038 of 664,199 trips (27%) have no start
station and 184,185 have no end station. They are dockless e-bike trips that
started or ended away from a dock.

**Impact.** Treating a missing station as invalid would remove a quarter of
all ridership and bias every total toward classic bikes.

**Solution.** Silver keeps these trips and flags them with
`has_start_station` and `has_end_station`. Gold maps them to a dedicated
"no station" row in `dim_station`. Station rankings exclude that row, while
ridership totals include every trip.

**Validation.** Unit tests check that a trip with no station reaches silver
and keeps its flag. The quality checks confirm every trip finds a row in
`dim_station`.

**Trade-off.** Station-level demand covers docked trips only.

## Two identifiers for the same station

**Problem.** Trip files identify a station by a short code such as `31818`.
The live feed identifies it by a UUID and carries the short code in a
separate `short_name` field.

**Impact.** A join on the obvious ID columns matches nothing.

**Solution.** `dim_station` is keyed on the short code. Availability facts
reach it through the UUID, which is unique per row in the dimension.
Stations that appear only in trip history are added from the trip records.

**Validation.** The quality checks confirm that every trip station exists in
`dim_station` and that the dimension has one row per key.

**Trade-off.** Stations with no row in the live feed have no capacity or
region.

## Repeated GBFS snapshots

**Problem.** The feed refreshes about once a minute. A poll can still return a
document identical to the previous poll, and a station can report the same
counts for hours.

**Impact.** Removing rows with identical counts would delete real
observations of a quiet station. Keeping identical documents twice would
double-count.

**Solution.** The grain is the station plus the feed's own `last_updated`
time. Two rows with the same station and `last_updated` are the same
observation, and the later copy is quarantined as a duplicate. Rows with
different `last_updated` values are kept even when every count matches.

**Validation.** `tests/unit/test_station_status.py` covers both cases. The
quality checks confirm one row per station and snapshot in gold.

**Trade-off.** A station that does not report for a long time still appears in
every snapshot with an old `last_reported`. Those rows are flagged `is_stale`
and excluded from the availability rates.

## Masked column exposed through quarantine

**Problem.** `silver.trips` masks `ride_id` for analysts. The first design put
the trip quarantine table in the silver schema. Each quarantined row keeps the
original record, including the raw `ride_id`.

**Impact.** Analysts can read the silver schema, so they could have read
unmasked IDs in the quarantine table.

**Solution.** Quarantine tables live in their own `quarantine` schema, and
only engineers can read it.

**Validation.** `tests/unit/test_deployment_scripts.py` checks that no group
other than the engineers is granted access to bronze or quarantine.

**Trade-off.** Analysts who want to see why rows were rejected need an
engineer to look for them.

## Results that depended on the machine's time zone

**Problem.** A gold test passed on one machine and failed on another.
PySpark converts naive Python datetimes using the machine's local time zone,
even when the Spark session time zone is UTC.

**Impact.** Local hours in `fact_station_availability` were shifted in the
test data, so the test checked the wrong value.

**Solution.** The test session pins the Python process and the JVM to UTC.
Databricks runs in UTC by default, so production behaviour is unchanged.

**Validation.** The full suite passes locally and in CI.

**Trade-off.** None.
