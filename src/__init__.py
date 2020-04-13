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
from .importer import CovidImporter, CountryImporter
from .utils import map_date

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

    @app.route('/import_countries')
    def import_countries():
        pw = request.args.get("pw")

        if not pw == current_app.config["IMPORTER_SECRET_KEY"]:
            current_app.logger.info("Wrong password entered for covid import")
            return jsonify(404)

        importer = CountryImporter()
        importer.start()

        return jsonify(True)

    
    @app.route('/covid19/import_data')
    def import_covid_data():
        pw = request.args.get("pw")

        if not pw == current_app.config["IMPORTER_SECRET_KEY"]:
            current_app.logger.info("Wrong password entered for covid import")
            return jsonify(404)

        importer = CovidImporter()
        importer.start()

        return jsonify(True)

    @app.route('/countries')
    def countries():
        country_filter = request.args.get("country")
        country_code_filter = request.args.get("code")

        where = ""
        if country_filter:
            where = f" WHERE LOWER(name) = '{country_filter.lower()}'"
        elif country_code_filter:
            where = f" WHERE LOWER(country_code) = '{country_code_filter.lower()}'"

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

            if country_filter or country_code_filter:
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
            sum(cc.confirmed),
            sum(cc.deaths),
            sum(cc.recovered),
            max(ct.last_update),
            sum(ct.delta_confirmed),
            sum(ct.delta_recovered),
            sum(ct.delta_deaths)
        FROM cases_total ct
        JOIN cases_country cc ON ct.country_code = cc.country_code
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
            "date": map_date(last_update)
        }

        return jsonify(result)

    @app.route('/covid19/cases-total')
    def cases_by_countries():
        country_filter = request.args.get("country")
        country_code_filter = request.args.get("code")
        worldwide = request.args.get("worldwide")

        if worldwide:
            return get_cases_worldwide()

        cursor = db.get_db().cursor()

        where = ""
        if country_filter:
            where = f"WHERE LOWER(ct.country_region) = '{country_filter.lower()}'"
        elif country_code_filter:
            where = f" WHERE LOWER(ct.country_code) = '{country_code_filter.lower()}'"

        query = f"""
        SELECT 
            ct.country_region,
            cc.confirmed,
            cc.deaths,
            cc.recovered,
            cc.active,
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
        JOIN countries c ON ct.country_code = c.code
        JOIN cases_country cc ON cc.country_code = ct.country_code
        { where }
        ORDER BY c.name
        """
        cursor.execute(query)

        result = []
        country = {}
        cases = {}
        for row in cursor.fetchall():
            country_region, confirmed, deaths, recovered, active, last_update, delta_confirmed, delta_recovered, delta_deaths, code, name, population, life_expectancy, continent, capital, population_density, avg_temperature = row

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
                "confirmed": getIntOrNone(confirmed),
                "deaths": getIntOrNone(deaths),
                "recovered": getIntOrNone(recovered),
                "active": getIntOrNone(active),
                "delta_confirmed": getIntOrNone(delta_confirmed),
                "delta_recovered": getIntOrNone(delta_recovered),
                "delta_deaths": getIntOrNone(delta_deaths),
                "date": last_update
            }

            if not country_filter or not country_code_filter:
                result.append({
                    "country": country,
                    "cases": cases,
                })
            else:
                result = cases

        return jsonify(result)

    def getIntOrNone(value):
        if isinstance(value, int):
            return value
        return None

    @app.route('/covid19/cases-daily')
    def cases_total_days():
        country_filter = request.args.get("country")
        country_code_filter = request.args.get("code")

        where = ""
        if country_filter:
            where = f"WHERE LOWER(country_region) = '{country_filter.lower()}'"
        elif country_code_filter:
            where = f"WHERE LOWER(country_code) = '{country_code_filter.lower()}'"
        
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

    ## Start jobs
    #covid_importer = CovidImporter()
    #import_scheduler = BackgroundScheduler()
    #import_scheduler.add_job(func=covid_importer.scheduled_start, args=[app], trigger="interval", seconds=20)
    #import_scheduler.start()
    ## Shut down the scheduler when exiting the app
    #atexit.register(lambda: import_scheduler.shutdown())


    return app

app = create_app()
