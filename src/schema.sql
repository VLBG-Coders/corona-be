drop table if exists cases_time;

CREATE TABLE IF NOT EXISTS cases_time(
    country_region TEXT,
    last_update DATETIME,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    active INTEGER,
    delta_confirmed INTEGER,
    delta_recovered INTEGER
);