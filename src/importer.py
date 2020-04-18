import re
import json
import csv
import requests
import time
import logging
from . import db
from .utils import map_date
from flask import current_app

COVID_MASTER_BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
COUNTRY_LUT_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv"
CSV_BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/web-data/data/"
COUNTRY_JSON_BASE_URL = "https://raw.githubusercontent.com/samayo/country-json/master/src/"
INSERT_BATCH = 100
UPDATE_BATCH = 500

logger = logging.getLogger('waitress')
logger.setLevel(logging.INFO)

def do_bulk_insert(table_name, data, header):
    # Replace empty strings with None.
    data = tuple(x if x else None for x in data)
    placeholder = ["?" for i in range(len(header))]
    query = f'INSERT INTO {table_name} ({",".join(header).lower()}) VALUES ({",".join(placeholder)})'
    db.get_db().executemany(query, data)
    db.get_db().commit()

class CovidImporter:

    def __init__(self):
        self.init_lookup_table()

    def init_lookup_table(self):
        data = self._download_csv(COUNTRY_LUT_URL)
        cr = csv.reader(data, delimiter=',', quotechar='"')
        count = 0
        result = []

        self.country_lookup = {}
        self.country_lookup_iso3 = {}

        for row in cr:
            count += 1
            if count == 1:
                # CSV Header
                header = row

                index_country_region = header.index("Country_Region")
                index_iso2 = header.index("iso2")
                index_iso3 = header.index("iso3")

                continue

            if len(row) != len(header):
                continue

            self.country_lookup[row[index_country_region]] = row[index_iso2]
            self.country_lookup_iso3[row[index_iso3]] = row[index_iso2]

    def _get_country_code(self, country_region):
        """ Returns country code by given country_region.
        """
        if country_region in self.country_lookup:
            return self.country_lookup[country_region]
        return None

    def _get_country_code_by_iso3(self, iso3):
        """ Returns country code by given country_region.
        """
        if iso3 in self.country_lookup_iso3:
            return self.country_lookup_iso3[iso3]
        return None

    def _read_and_import_csv(self, data, table_name):
        """ Import all data. no delta import.
        """
        cr = csv.reader(data, delimiter=',', quotechar='"')
        count = 0
        result = []
        insert_data = []
        header = None
        additional_headers = ["country_code"]

        temp_table_name = table_name + "_new"

        for row in cr:
            count += 1
            if count == 1:
                # CSV Header
                header = row
                header = header + additional_headers
                # Delete all data
                query = f"DROP TABLE IF EXISTS {temp_table_name}"
                db.get_db().execute(query)
                query = f"CREATE TABLE {temp_table_name} AS SELECT * FROM {table_name} WHERE 0"
                db.get_db().execute(query)
                last_update_index = "Last_Update" in header and header.index(
                    "Last_Update") or False

                index_country_region = header.index("Country_Region")
                try:
                    index_iso3 = header.index("ISO3")
                except ValueError:
                    index_iso3 = header.index("iso3")

                continue

            if len(row) != len(header)-len(additional_headers):
                continue

            # handle date dd/mm/yy
            if last_update_index is not False:
                row[last_update_index] = map_date(row[last_update_index])

            row.append(self._get_country_code_by_iso3(row[index_iso3]))

            insert_data.append(tuple(row))
            if count % INSERT_BATCH == 0:
                do_bulk_insert(temp_table_name, insert_data, header)
                insert_data = []

            result.append(row)

        if insert_data:
            do_bulk_insert(temp_table_name, insert_data, header)

        query = f"DROP TABLE {table_name}"
        db.get_db().execute(query)
        query = f"ALTER TABLE {temp_table_name} RENAME TO {table_name}"
        db.get_db().execute(query)

        return result

    def _read_and_import_cases_total(self, data):
        """ Import all data. no delta import.
        """
        cr = csv.reader(data, delimiter=',', quotechar='"')
        count = 0
        result = []
        insert_data = []
        header = None
        table_name = "cases_total"

        temp_table_name = table_name + "_new"

        last_country: str = None
        last_deaths: int = 0
        last_row: list = None

        additional_headers = ["Delta_Deaths", "country_code"]

        for row in cr:
            count += 1
            if count == 1:
                # CSV Header
                header = row
                header = header + additional_headers
                # Delete all data
                query = f"DROP TABLE IF EXISTS {temp_table_name}"
                db.get_db().execute(query)
                query = f"CREATE TABLE {temp_table_name} AS SELECT * FROM {table_name} WHERE 0"
                db.get_db().execute(query)
                last_update_index = "Last_Update" in header and header.index(
                    "Last_Update")
                country_index = "Country_Region" in header and header.index(
                    "Country_Region")
                deaths_index = "Deaths" in header and header.index("Deaths")
                index_iso3 = header.index("iso3")
                index_province = header.index("Province_State")

                continue

            if len(row) != len(header)-len(additional_headers):
                continue

            if row[index_province] != "":
                continue

            current_country = row[country_index]
            current_deaths = int(row[deaths_index])

            # Assumption: List is ordered by countries and dates, so each time the country
            # changes we have got the latest record of a country in the record before.
            if last_row and last_country != current_country:
                last_row[last_update_index] = map_date(row[last_update_index])
                insert_data.append(tuple(last_row))
                last_deaths = 0

            # Save row data for next loop run
            row.append(current_deaths-last_deaths)
            row.append(self._get_country_code_by_iso3(row[index_iso3]))
            last_deaths = current_deaths
            last_country = current_country
            last_row = row

            if len(insert_data) % INSERT_BATCH == 0:
                do_bulk_insert(temp_table_name, insert_data, header)
                insert_data = []

            result.append(row)

        last_row[last_update_index] = map_date(last_row[last_update_index])
        insert_data.append(last_row)
        do_bulk_insert(temp_table_name, insert_data, header)

        query = f"DROP TABLE {table_name}"
        db.get_db().execute(query)
        query = f"ALTER TABLE {temp_table_name} RENAME TO {table_name}"
        db.get_db().execute(query)

        return result

    def _read_and_import_master_timeseries(self, dataset_name, data):
        """Imports timeseries data from master branch. either confirmed, deaths or
        recovered and updated the cases_time table.
        """

        def do_bulk_update(data):
            query = f"""
            UPDATE cases_time
              SET {dataset_name} = ?,
              delta_{dataset_name} = ?
            WHERE country_region = ?
              AND last_update =  ?
            """
            db.get_db().executemany(query, data)
            db.get_db().commit()

        def update_cases_total(data):
            query = f"UPDATE cases_total SET delta_{dataset_name} = ? where country_region = ?"
            db.get_db().executemany(query, data)
            db.get_db().commit()

        cr = csv.reader(data, delimiter=',', quotechar='"')
        row_count = 0
        result = []
        insert_data = []
        cases_total_data = []
        header = None
        time_table_name = "cases_time"
        additional_headers = []

        countries_with_province = {}

        last_country = None
        for row in cr:
            row_count += 1
            if row_count == 1:
                # CSV Header
                header = row
                header = header + additional_headers
                index_country = header.index("Country/Region")
                index_province = header.index("Province/State")
                continue

            if len(row) != len(header)-len(additional_headers):
                continue

            current_country = row[index_country]
            current_province = row[index_province]

            last_value = 0
            last_country = None
            last_province = None
            # for each country iterate through all datesl
            for index, column in enumerate(header): 
                date = map_date(column)

                # not a date column, continue
                if not date:
                    continue

                current_value = int(row[index])
                # only use positive deltas.
                delta = max(current_value - last_value, 0) 

                last_value = current_value
                # if no province is set, we know we can store the data for the country.
                if current_province == "":
                    # check if in province list and remove it otherwise
                    if current_country in countries_with_province:
                        del countries_with_province[current_country]

                    insert_data.append((current_value, delta, current_country, date))
                else:
                    # sum up delta for all provinces
                    countries_with_province[current_country] = countries_with_province.get(current_country, {})
                    countries_with_province[current_country][date] = (
                        countries_with_province[current_country].get(date, {})
                    )
                    countries_with_province[current_country][date]["delta"] = (
                        countries_with_province[current_country][date].get("delta", 0) + delta
                    )
                    countries_with_province[current_country][date]["value"] = (
                        countries_with_province[current_country][date].get("value", 0) + current_value
                    )

            # For latest dataset update cases_total with delta
            if current_province == "":
                cases_total_data.append((delta, current_country))

            if insert_data and len(insert_data) % UPDATE_BATCH == 0:
                do_bulk_update(insert_data)
                update_cases_total(cases_total_data)
                cases_total_data = []
                insert_data = []

        # iterate through all non processed countries with provinces
        for country in countries_with_province:
            for date in countries_with_province.get(country):
                delta = countries_with_province.get(country).get(date, {}).get("delta")
                value = countries_with_province.get(country).get(date, {}).get("value")
                insert_data.append((value, delta, country, date))

            # update cases_total as well with latest delta
            cases_total_data.append((delta, country))

        if insert_data:
            do_bulk_update(insert_data)
            update_cases_total((cases_total_data))

        return result

    def _download_csv(self, url) -> list:
        """ Downloads csv from given url and returns it as decoded list.
        """
        download = requests.get(url)
        decoded_content = download.content.decode('utf-8')
        return decoded_content.split("\n")

    def _download_covid_csv(self, name) -> list:
        """ Downloads csv from covid19 data source and returns it as decoded list.
        """
        return self._download_csv(CSV_BASE_URL + name)

    def _download_and_import_covid_csv(self, name):
        data = self._download_covid_csv(name + ".csv")
        result = self._read_and_import_csv(data, name)
        current_app.logger.info("Imported %s entries for %s", len(result), name)

        return data

    def start(self):
        start_time = time.monotonic()
        current_app.logger.info("Running import of covid 19 data.")

        data = self._download_and_import_covid_csv("cases_time")
        self._read_and_import_cases_total(data)
        self._download_and_import_covid_csv("cases_country")

        data = self._download_csv(COVID_MASTER_BASE_URL + "time_series_covid19_recovered_global.csv")
        self._read_and_import_master_timeseries("recovered", data)

        current_app.logger.info("Import finished in %s seconds",
                                time.monotonic() - start_time)

    def scheduled_start(self, app):
        """ called by ap backgroundscheduler
        """
        with app.app_context():
            db.init_app(app)
            self.start()

class CountryImporter:

    def _download_country_json(self, name) -> list:
        """ Downloads country json and returns it as string.
        """
        download = requests.get(COUNTRY_JSON_BASE_URL + name)
        decoded_content = download.content.decode('utf-8')
        return json.loads(decoded_content)

    def _import_country_data(self):
        countries = self._download_country_json("country-by-abbreviation.json")
        population_list = self._download_country_json("country-by-population.json")
        life_expectancy = self._download_country_json("country-by-life-expectancy.json")
        continents = self._download_country_json("country-by-continent.json")
        capital_cities = self._download_country_json("country-by-capital-city.json")
        population_density = self._download_country_json(
            "country-by-population-density.json")
        avg_temperature = self._download_country_json(
            "country-by-yearly-average-temperature.json")

        query = f"DELETE FROM countries"
        db.get_db().execute(query)

        data = []
        result = []
        count = 0
        dbPropertyMapping = [
            "code",
            "name",
            "population",
            "life_expectancy",
            "continent",
            "capital",
            "population_density",
            "avg_temperature"
        ]

        for country in countries:
            population_obj = next((x for x in population_list if x.get(
                "country") == country.get("country")), {})
            life_expectancy_obj = next(
                (x for x in life_expectancy if x.get("country") == country.get("country")), {})
            continents_obj = next((x for x in continents if x.get(
                "country") == country.get("country")), {})
            capital_cities_obj = next(
                (x for x in capital_cities if x.get("country") == country.get("country")), {})
            population_density_obj = next((x for x in population_density if x.get(
                "country") == country.get("country")), {})
            avg_temperature_obj = next(
                (x for x in avg_temperature if x.get("country") == country.get("country")), {})

            obj = (
                country.get("abbreviation"),
                country.get("country"),
                population_obj.get("population"),
                life_expectancy_obj.get("expectancy"),
                continents_obj.get("continent"),
                capital_cities_obj.get("city"),
                population_density_obj.get("density"),
                avg_temperature_obj.get("temperature")
            )

            data.append(obj)
            result.append(obj)

            count += 1
            if count % INSERT_BATCH == 0:
                do_bulk_insert("countries", data, dbPropertyMapping)
                data = []

        do_bulk_insert("countries", data, dbPropertyMapping)

        return True

    def start(self):
        start_time = time.monotonic()
        current_app.logger.info("Running import of country data.")

        self._import_country_data()

        current_app.logger.info("Import finished in %s seconds",
                                time.monotonic() - start_time)
