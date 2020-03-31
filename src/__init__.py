import os
import re
import sqlite3
import json
import csv
import requests
from . import db


from flask import Flask, g, request, jsonify

CSV_BASE_URL="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/web-data/data/"
COUNTRY_JSON_BASE_URL="https://raw.githubusercontent.com/samayo/country-json/master/src/"
INSERT_BATCH = 100

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
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

        for row in cr:
            count+=1
            if count == 1:
                # CSV Header
                header = row
                # Delete all data
                query = f"DELETE FROM {table_name}"
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
                do_bulk_insert(table_name, insert_data, header)
                insert_data = []

            result.append(row)

        if insert_data:
            do_bulk_insert(table_name, insert_data, header)

        return json.dumps(result)

    def download_csv(name):
        download = requests.get(CSV_BASE_URL + name)
        decoded_content = download.content.decode('utf-8')
        return decoded_content.split("\n")


    def download_country_json(name):
        download = requests.get(COUNTRY_JSON_BASE_URL + name)
        decoded_content = download.content.decode('utf-8')
        return decoded_content


    @app.route('/import_countries')
    def import_countries():
        json_data = download_country_json("country-by-abbreviation.json")
        countries = json.loads(json_data)

        json_data = download_country_json("country-by-population.json")
        population_list = (json.loads(json_data))

        json_data = download_country_json("country-by-life-expectancy.json")
        life_expectancy = (json.loads(json_data))

        json_data = download_country_json("country-by-continent.json")
        continents = (json.loads(json_data))

        json_data = download_country_json("country-by-capital-city.json")
        capital_cities = (json.loads(json_data))

        json_data = download_country_json("country-by-population-density.json")
        population_desity = (json.loads(json_data))

        json_data = download_country_json("country-by-yearly-average-temperature.json")
        avg_temperature = (json.loads(json_data))

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
            population_desity_obj = next((x for x in population_desity if x.get("country") == country.get("country")), {})
            avg_temperature_obj = next((x for x in avg_temperature if x.get("country") == country.get("country")), {})

            obj = (
                country.get("abbreviation"),
                country.get("country"),
                population_obj.get("population"),
                life_expectancy_obj.get("expectancy"),
                continents_obj.get("continent"),
                capital_cities_obj.get("city"),
                population_desity_obj.get("density"),
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

    # testing only
    @app.route('/import_test')
    def import_test():
        with open('cases_time.csv') as csvfile:
            data = read_and_import_csv(csvfile, "cases_time")
        return data

    @app.route('/import_covid19')
    def import_csv():
        imported = {}
        data = download_csv("cases_time.csv")
        read_and_import_csv(data, "cases_time")
        data = download_csv("cases_country.csv")
        read_and_import_csv(data, "cases_country")

        return jsonify("True")

    @app.route('/countries')
    def countries():
        country = request.args.get("country")
        query = f"""
        SELECT code, name, population, life_expectancy, continent, capital, population_density, avg_temperature
        FROM countries
        ORDER BY name ASC
        """

        if country:
            query = f"""
            SELECT code, name, population, life_expectancy, continent, capital, population_density, avg_temperature
            FROM countries
            WHERE LOWER(name) = '{country.lower()}'
            ORDER BY name ASC
            """

        cursor = db.get_db().cursor()
        cursor.execute(query)

        result = []
        for row in cursor.fetchall():
            code, name, population, life_expectancy, continent, capital, population_density, avg_temperature = row
            result.append({
                "code": code,
                "name": name,
                "population": population,
                "life_expectancy": life_expectancy,
                "continent": continent,
                "capital": capital,
                "population_density": population_density,
                "avg_temperature": avg_temperature
            })

        return jsonify(result)

    @app.route('/cases-by-country')
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

    @app.route('/cases-total')
    def cases_by_countries():
        country = request.args.get("country")

        cursor = db.get_db().cursor()
        # query = f"""
        # SELECT cc.country_region, cc.confirmed, cc.deaths, cc.recovered, ct.last_update, ct.delta_confirmed, ct.delta_recovered
        # FROM cases_country cc
        # JOIN cases_time ct ON cc.country_region = ct.country_region
        # LEFT OUTER JOIN cases_time ct2 ON ct2.country_region = ct.country_region and (ct2.last_update > ct.last_update
        # OR (ct.country_region <> ct2.country_region  and ct2.last_update = ct.last_update))
        # WHERE ct2.country_region is ct.country_region
        # ORDER BY country_region ASC
        # """
        ## this is slow and needs to be improved

        query = f"""
        SELECT cc.country_region,
        cc.confirmed,
        cc.deaths,
        cc.recovered,
        ct.last_update,
        ct.delta_confirmed,
        ct.delta_recovered,
        c.code,
        c.name,
        c.population,
        c.life_expectancy,
        c.continent,
        c.capital,
        c.population_density,
        c.avg_temperature
        FROM cases_country cc
        JOIN cases_time ct ON cc.country_region = ct.country_region
        JOIN countries c ON cc.country_region = c.name
        WHERE ct.last_update IN (
            SELECT ct2.last_update
            FROM cases_time ct2
            WHERE ct2.country_region = ct.country_region
            ORDER BY ct2.last_update DESC
            LIMIT 1
        )
        """

        if country:
            query = f"""
            SELECT cc.country_region,
            cc.confirmed,
            cc.deaths,
            cc.recovered,
            ct.last_update,
            ct.delta_confirmed,
            ct.delta_recovered,
            c.code,
            c.name,
            c.population,
            c.life_expectancy,
            c.continent,
            c.capital,
            c.population_density,
            c.avg_temperature
            FROM cases_country cc
            JOIN cases_time ct ON cc.country_region = ct.country_region
            JOIN countries c ON cc.country_region = c.name
            WHERE ct.last_update IN (
                SELECT ct2.last_update
                FROM cases_time ct2
                WHERE ct2.country_region = ct.country_region AND LOWER(c.name) = '{country.lower()}'
                ORDER BY ct2.last_update DESC
                LIMIT 1
                )
            """

        cursor.execute(query)

        result = []
        for row in cursor.fetchall():
            country_region, confirmed, deaths, recovered, last_update, delta_confirmed, delta_recovered, code, name, population, life_expectancy, continent, capital, population_density, avg_temperature = row

            result.append({
                "country": {
                    "code": code,
                    "name": name,
                    "population": population,
                    "life_expectancy": life_expectancy,
                    "continent": continent,
                    "capital": capital,
                    "population_density": population_density,
                    "avg_temperature": avg_temperature
                },
                "cases": {
                    "confirmed": confirmed,
                    "deaths": deaths,
                    "recovered": recovered,
                    "delta_confirmed": delta_confirmed,
                    "delta_recovered": delta_recovered,
                    "date": last_update
                }
            })

        return jsonify(result)

    @app.route('/cases-timeline')
    def cases_total_days():
        country = request.args.get("country")
        query = f"""
        SELECT SUM(confirmed) as confirmed, SUM(deaths) as deaths, SUM(recovered) as recovered, last_update
        FROM cases_time
        GROUP BY last_update
        ORDER BY last_update DESC
        """

        if country:
            query = f"""
            SELECT SUM(confirmed) as confirmed, SUM(deaths) as deaths, SUM(recovered) as recovered, last_update
            FROM cases_time
            WHERE LOWER(country_region) = '{country.lower()}'
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

    return app

app = create_app()
