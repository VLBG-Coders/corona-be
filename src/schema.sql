DROP TABLE IF EXISTS cases_time;

CREATE TABLE IF NOT EXISTS cases_time (
    country_region TEXT,
    country_code TEXT,
    last_update DATETIME,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    active INTEGER,
    delta_confirmed INTEGER,
    delta_recovered INTEGER,
    incident_rate REAL,
    people_tested INTEGER,
    people_hospitalized INTEGER,
    province_state TEXT,
    FIPS TEXT,
    UID TEXT,
    iso3 TEXT,
    Report_Date_String DATETIME
);

DROP TABLE IF EXISTS cases_total;
CREATE TABLE IF NOT EXISTS cases_total (
    country_region TEXT,
    country_code TEXT,
    last_update DATETIME,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    active INTEGER,
    delta_confirmed INTEGER,
    delta_recovered INTEGER,
    delta_deaths INTEGER,
    incident_rate REAL,
    people_tested INTEGER,
    people_hospitalized INTEGER,
    province_state TEXT,
    FIPS TEXT,
    UID TEXT,
    iso3 TEXT,
    Report_Date_String DATETIME
);

DROP TABLE IF EXISTS cases_country;

CREATE TABLE IF NOT EXISTS cases_country (
    country_region TEXT,
    country_code TEXT,
    last_update DATETIME,
    lat REAL,
    long_ REAL,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    active INTEGER,
    incident_rate REAL,
    people_tested INTEGER,
    people_hospitalized INTEGER,
    mortality_rate REAL,
    UID TEXT,
    iso3 TEXT
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
