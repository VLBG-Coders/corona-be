import os
import re
import sqlite3
import json
import csv
import requests
import time
from . import db
from flask_cors import CORS, cross_origin
from flask import Flask, g, request, jsonify, current_app
import logging
import atexit

from apscheduler.schedulers.background import BackgroundScheduler

CSV_BASE_URL="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/web-data/data/"
COUNTRY_JSON_BASE_URL="https://raw.githubusercontent.com/samayo/country-json/master/src/"
INSERT_BATCH = 100

logger = logging.getLogger('waitress')
logger.setLevel(logging.INFO)

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    CORS(app)

    app.config.from_mapping(
        SECRET_KEY='dev',
        IMPORTER_SECRET_KEY = 'c0v1d19',
        DATABASE=os.path.join(app.instance_path, 'corona.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # db init
    db.init_app(app)

    def do_bulk_insert(table_name, data, header):
        # Replace empty strings with None.
        data = tuple(x if x else None for x in data)
        placeholder = ["?" for i in range(len(header))]
        query = f'INSERT INTO {table_name} ({",".join(header).lower()}) VALUES ({",".join(placeholder)})'
        db.get_db().executemany(query, data)
        db.get_db().commit()

    def read_and_import_csv(data, table_name):
        """ Import all data. no delta import.
        """
        cr = csv.reader(data, delimiter=',', quotechar='"')
        count=0
        result = []
        insert_data = []
        header = None

        temp_table_name = table_name + "_new"

        for row in cr:
            count+=1
            if count == 1:
                # CSV Header
                header = row
                # Delete all data
                query = f"DROP TABLE IF EXISTS {temp_table_name}"
                db.get_db().execute(query)
                query = f"CREATE TABLE {temp_table_name} AS SELECT * FROM {table_name} WHERE 0"
                db.get_db().execute(query)
                last_update_index = "Last_Update" in header and header.index("Last_Update") or False
                continue

            if len(row) != len(header):
                continue

            # handle date dd/mm/yy
            if last_update_index is not False:
                re_result = re.search('(\d{1,2})/(\d{1,2})/(\d{2})', row[last_update_index])
                if re_result:
                    month, day, year = re_result.groups()
                    # attention: this will only work for 80 years! ;)
                    row[last_update_index] = f"20{year}-{int(month):02}-{int(day):02}"

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

    def read_and_import_cases_total(data):
        """ Import all data. no delta import.
        """
        cr = csv.reader(data, delimiter=',', quotechar='"')
        count=0
        result = []
        insert_data = []
        header = None
        table_name="cases_total"

        temp_table_name = table_name + "_new"

        last_country: str = None
        last_deaths: int = 0
        last_row: list = None

        for row in cr:
            count+=1
            if count == 1:
                # CSV Header
                header = row
                header.append("Delta_Deaths")
                # Delete all data
                query = f"DROP TABLE IF EXISTS {temp_table_name}"
                db.get_db().execute(query)
                query = f"CREATE TABLE {temp_table_name} AS SELECT * FROM {table_name} WHERE 0"
                db.get_db().execute(query)
                last_update_index = "Last_Update" in header and header.index("Last_Update")
                country_index = "Country_Region" in header and header.index("Country_Region")
                deaths_index = "Deaths" in header and header.index("Deaths")

                continue

            if len(row) != len(header)-1:
                continue

            current_country = row[country_index]
            current_deaths = int(row[deaths_index])

            # Assumption: List is ordered by countries and dates, so each time the country
            # changes we have got the latest record of a country in the record before.
            if last_row and last_country != current_country:
                # handle date dd/mm/yy
                if last_update_index is not False:
                    re_result = re.search('(\d{1,2})/(\d{1,2})/(\d{2})', row[last_update_index])
                    if re_result:
                        month, day, year = re_result.groups()
                        # attention: this will only work for 80 years! ;)
                        last_row[last_update_index] = f"20{year}-{int(month):02}-{int(day):02}"
                insert_data.append(tuple(last_row))
                last_deaths = 0

            # Save row data for next loop run
            row.append(current_deaths-last_deaths)
            last_deaths = current_deaths
            last_country = current_country
            last_row = row

            if len(insert_data) % INSERT_BATCH == 0:
                do_bulk_insert(temp_table_name, insert_data, header)
                insert_data = []

            result.append(row)


        insert_data.append(last_row)
        do_bulk_insert(temp_table_name, insert_data, header)

        query = f"DROP TABLE {table_name}"
        db.get_db().execute(query)
        query = f"ALTER TABLE {temp_table_name} RENAME TO {table_name}"
        db.get_db().execute(query)

        return result

    def download_csv(name) -> list:
        """ Downloads csv from covid19 data source. and returns it as decoded list.
        """
        download = requests.get(CSV_BASE_URL + name)
        decoded_content = download.content.decode('utf-8')
        return decoded_content.split("\n")


    def download_country_json(name) -> list:
        """ Downloads country json and returns it as string.
        """
        download = requests.get(COUNTRY_JSON_BASE_URL + name)
        decoded_content = download.content.decode('utf-8')
        return json.loads(decoded_content)


    @app.route('/import_countries')
    def import_countries():
        pw = request.args.get("pw")

        if not pw == current_app.config["IMPORTER_SECRET_KEY"]:
            return jsonify(404)

        countries = download_country_json("country-by-abbreviation.json")
        population_list = download_country_json("country-by-population.json")
        life_expectancy = download_country_json("country-by-life-expectancy.json")
        continents = download_country_json("country-by-continent.json")
        capital_cities = download_country_json("country-by-capital-city.json")
        population_density = download_country_json("country-by-population-density.json")
        avg_temperature = download_country_json("country-by-yearly-average-temperature.json")

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
            population_obj = next((x for x in population_list if x.get("country") == country.get("country")), {})
            life_expectancy_obj = next((x for x in life_expectancy if x.get("country") == country.get("country")), {})
            continents_obj = next((x for x in continents if x.get("country") == country.get("country")), {})
            capital_cities_obj = next((x for x in capital_cities if x.get("country") == country.get("country")), {})
            population_density_obj = next((x for x in population_density if x.get("country") == country.get("country")), {})
            avg_temperature_obj = next((x for x in avg_temperature if x.get("country") == country.get("country")), {})

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

        return jsonify(True)
    
    def download_and_import_covid_csv(name):
        data = download_csv(name + ".csv")
        result = read_and_import_csv(data, name)
        current_app.logger.info("Imported %s entries for %s", len(result), name)
        return data

    def start_covid_import():
        start_time = time.monotonic()
        current_app.logger.info("Running import of covid 19 data.")

        data = download_and_import_covid_csv("cases_time")
        read_and_import_cases_total(data)
        download_and_import_covid_csv("cases_country")


        current_app.logger.info("Import finished in %s seconds", time.monotonic() - start_time)

    @app.route('/covid19/import_data')
    def import_covid_data():
        pw = request.args.get("pw")

        if not pw == current_app.config["IMPORTER_SECRET_KEY"]:
            current_app.logger.info("Wrong password entered for covid import")
            return jsonify(404)

        start_covid_import()

        return jsonify(True)

    @app.route('/countries')
    def countries():
        countryFilter = request.args.get("country")

        where = ""
        if countryFilter:
            where = f" WHERE LOWER(name) = '{countryFilter.lower()}'"

        query = f"""
        SELECT code, name, population, life_expectancy, continent, capital, population_density, avg_temperature
        FROM countries
        {where}
        ORDER BY name ASC
        """

        cursor = db.get_db().cursor()
        cursor.execute(query)

        result = []
        country = {}
        for row in cursor.fetchall():
            code, name, population, life_expectancy, continent, capital, population_density, avg_temperature = row
            country = {
                "code": code,
                "name": name,
                "population": population,
                "life_expectancy": life_expectancy,
                "continent": continent,
                "capital": capital,
                "population_density": population_density,
                "avg_temperature": avg_temperature
            }
            result.append(country)

            if countryFilter:
                result = country

        return jsonify(result)

    @app.route('/covid19/cases-by-country')
    def cases_by_country():
        country = request.args.get("country")

        if not country:
            return "please provide a country", 400

        cursor = db.get_db().cursor()
        queryCases = f"""
        SELECT country_region, confirmed, deaths, recovered, last_update
        FROM cases_time
        WHERE LOWER(country_region) = '{country.lower()}'
        ORDER BY last_update ASC
        """
        cursor.execute(queryCases)

        cursor1 = db.get_db().cursor()
        queryCountry = f"""
        SELECT code, name, population, life_expectancy, continent, capital, population_density, avg_temperature
        FROM countries
        WHERE LOWER(name) = '{country.lower()}'
        """
        cursor1.execute(queryCountry)

        countries = []
        for row in cursor1.fetchall():
            code, name, population, life_expectancy, continent, capital, population_density, avg_temperature = row
            countries.append({
                "code": code,
                "name": name,
                "population": population,
                "life_expectancy": life_expectancy,
                "continent": continent,
                "capital": capital,
                "population_density": population_density,
                "avg_temperature": avg_temperature
            })

        result = {}
        count = 0
        for row in cursor.fetchall():
            country_region, confirmed, deaths, recovered, last_update = row
            count += 1
            if count == 1:
                result = {
                    "country": countries[0],
                    "timeline": []
                }

            result.get("timeline").append({
                "date": last_update,
                "confirmed": confirmed,
                "deaths": deaths,
                "recovered": recovered,
            })

        return jsonify(result)

    def get_cases_worldwide():
        cursor = db.get_db().cursor()
        query = f"""
        SELECT
            sum(ct.confirmed),
            sum(ct.deaths),
            sum(ct.recovered),
            max(ct.last_update),
            sum(ct.delta_confirmed),
            sum(ct.delta_recovered),
            sum(ct.delta_deaths)
        FROM cases_total ct
        """
        cursor.execute(query)

        result = {}
        row = cursor.fetchone()
        confirmed, deaths, recovered, last_update, delta_confirmed, delta_recovered, delta_deaths = row

        result = {
            "confirmed": confirmed,
            "deaths": deaths,
            "recovered": recovered,
            "delta_confirmed": delta_confirmed,
            "delta_recovered": delta_recovered,
            "delta_deaths": delta_deaths,
            "date": last_update
        }

        return jsonify(result)

    @app.route('/covid19/cases-total')
    def cases_by_countries():
        countryFilter = request.args.get("country")
        worldwide = request.args.get("worldwide")

        if worldwide:
            return get_cases_worldwide()

        cursor = db.get_db().cursor()

        where = ""
        if countryFilter:
            where = f"WHERE LOWER(c.name) = '{countryFilter.lower()}'"

        query = f"""
        SELECT 
            ct.country_region,
            ct.confirmed,
            ct.deaths,
            ct.recovered,
            ct.last_update,
            ct.delta_confirmed,
            ct.delta_recovered,
            ct.delta_deaths,
            c.code,
            c.name,
            c.population,
            c.life_expectancy,
            c.continent,
            c.capital,
            c.population_density,
            c.avg_temperature
        FROM cases_total ct
        JOIN countries c ON ct.country_region = c.name
        { where }
        ORDER BY c.name
        """
        cursor.execute(query)

        result = []
        country = {}
        cases = {}
        for row in cursor.fetchall():
            country_region, confirmed, deaths, recovered, last_update, delta_confirmed, delta_recovered, delta_deaths, code, name, population, life_expectancy, continent, capital, population_density, avg_temperature = row

            country = {
                "code": code,
                "name": name,
                "population": population,
                "life_expectancy": life_expectancy,
                "continent": continent,
                "capital": capital,
                "population_density": population_density,
                "avg_temperature": avg_temperature
            }

            cases = {
                "confirmed": getValueOrNone(confirmed),
                "deaths": getValueOrNone(deaths),
                "recovered": getValueOrNone(recovered),
                "delta_confirmed": getValueOrNone(delta_confirmed),
                "delta_recovered": getValueOrNone(delta_recovered),
                "delta_deaths": getValueOrNone(delta_deaths),
                "date": last_update
            }

            if not countryFilter:
                result.append({
                    "country": country,
                    "cases": cases,
                })
            else:
                result = cases

        return jsonify(result)

    def getValueOrNone(value):
        return value if value else None;

    @app.route('/covid19/cases-daily')
    def cases_total_days():
        country = request.args.get("country")

        where = ""
        if country:
            where = f"WHERE LOWER(country_region) = '{country.lower()}'"
        
        query = f"""
        SELECT SUM(confirmed) as confirmed, SUM(deaths) as deaths, SUM(recovered) as recovered, last_update
        FROM cases_time
        { where }
        GROUP BY last_update
        ORDER BY last_update DESC
        """

        cursor = db.get_db().cursor()
        cursor.execute(query)

        result = []
        count = 0
        for row in cursor.fetchall():
            confirmed, deaths, recovered, last_update = row
            count += 1

            result.append({
                "confirmed": confirmed,
                "deaths": deaths,
                "recovered": recovered,
                "date": last_update
            })

        return jsonify(result)

    # Start jobs
    app_ctx = app.app_context()
    app_ctx.push()
    import_scheduler = BackgroundScheduler()
    import_scheduler.add_job(func=start_covid_import, trigger="interval", hours=2)
    import_scheduler.start()
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: import_scheduler.shutdown())


    return app

app = create_app()
