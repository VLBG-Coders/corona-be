# Corona BE

#### unreleased

- Added delta_confirmed and delta_recovered to cases-daily endpoint
- Added importer of recovered and delta recovered
- Added filter code to endpoints to filter for country_code (iso2)
- Adapted db schema according to current csv format
- Changed country mapping to use iso3 for identifying matches
- Fixed date in cases-total worldwide and added recovered
- Added `country_code` (iso2) to cases_total, cases_country and cases_time
- Moved importer into dedicated file
- Added table cases-total which contains delta_deaths
- Fixed import to cause almost no downtime
- Added background scheduler for triggering import every hour
- Added cors library
