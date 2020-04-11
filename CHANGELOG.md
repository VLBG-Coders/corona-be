# Corona BE

#### unreleased

- Added `country_code` (iso2) to cases_total, cases_country and cases_time
- Moved importer into dedicated file
- Added table cases-total which contains delta_deaths
- Fixed import to cause almost no downtime
- Added background scheduler for triggering import every hour
- Added cors library
