DROP TABLE IF EXISTS cases_time;

CREATE TABLE IF NOT EXISTS cases_time (
    country_region TEXT,
    last_update DATETIME,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    active INTEGER,
    delta_confirmed INTEGER,
    delta_recovered INTEGER
);

DROP TABLE IF EXISTS cases_country;

CREATE TABLE IF NOT EXISTS cases_country (
    country_region TEXT,
    last_update DATETIME,
    lat REAL,
    long_ REAL,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    active INTEGER
);

DROP TABLE IF EXISTS countries;
CREATE TABLE IF NOT EXISTS countries (
    id INTEGER,
    code TEXT,
    name TEXT,
    population INTEGER,
    life_expectancy REAL,
    continent TEXT,
    capital TEXT,
    population_density REAL,
    avg_temperature REAL
);
