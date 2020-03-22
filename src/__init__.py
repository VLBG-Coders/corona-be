import os
import sqlite3
import json
import csv
import requests
from . import db


from flask import Flask, g, request

CSV_BASE_URL="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/web-data/data/"
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

    def read_and_import_csv(data, table_name):
        """ Import all data. no delta import.
        """
        cr = csv.reader(data, delimiter=',', quotechar='"')
        count=0
        result = []
        header = None


        for row in cr:
            count+=1
            if count == 1:
                # CSV Header
                header = row
                # Delete all data
                query = f"DELETE FROM {table_name}"
                db.get_db().execute(query)
                continue

            if len(row) != len(header):
                continue

            result.append(row)

            placeholder = ["?" for i in range(len(header))]
            query = f'INSERT INTO {table_name} ({",".join(header).lower()}) VALUES ({",".join(placeholder)})'
            db.get_db().execute(query, row)
            db.get_db().commit()

        return json.dumps(result)

    def download_csv(name):
        download = requests.get(CSV_BASE_URL + name)
        decoded_content = download.content.decode('utf-8')
        return decoded_content.split("\n")

    # testing only
    @app.route('/import_test')
    def import_test():
        with open('cases_time.csv') as csvfile:
            data = read_and_import_csv(csvfile, "cases_time")
        return data

    @app.route('/import_csv')
    def import_csv():
        data = download_csv("cases_time.csv")
        read_and_import_csv(data, "cases_time")
        data = download_csv("cases_country.csv")
        read_and_import_csv(data, "cases_country")

        return True

    @app.route('/countries')
    def countries():
        cursor = db.get_db().cursor()
        query = f"SELECT DISTINCT country_region FROM cases_country  ORDER BY country_region ASC"
        cursor.execute(query)

        result = []
        for row in cursor.fetchall():
            result.append(row[0])

        return json.dumps(result)

    @app.route('/cases-by-country')
    def cases_by_country():
        country = request.args.get("country")
        
        if not country:
            return "please provide a country", 400

        cursor = db.get_db().cursor()
        query = f"SELECT country_region, confirmed, deaths, recovered, last_update FROM cases_time WHERE LOWER(country_region) = '{country.lower()}' ORDER BY last_update ASC"
        cursor.execute(query)

        result = {}
        count = 0
        for row in cursor.fetchall():
            country_region, confirmed, deaths, recovered, last_update = row
            count += 1
            if count == 1:
                result = {
                    "country": {
                        "name": country_region
                    },
                    "data": []
                }

            result.get("data").append({
                "date": last_update,
                "confirmed": confirmed,
                "deaths": deaths,
                "recovered": recovered,
            })

        return json.dumps(result)

    @app.route('/cases-by-countries')
    def cases_by_countries():
        cursor = db.get_db().cursor()
        # TODO: unfinished
        query = f"""
        SELECT cc.country_region, cc.confirmed, cc.deaths, cc.recovered, ct.last_update, ct.delta_confirmed, ct.delta_recovered
        FROM cases_country cc
        JOIN cases_time ct ON cc.country_region = ct.country_region
        join cases_time ct2 on ct2.country_region = ct.country_region and ct2.last_update > ct.last_update
        where ct2.country_region is null
        """
        cursor.execute(query)

        result = []
        count = 0
        for row in cursor.fetchall():
            country_region, confirmed, deaths, recovered, last_update, delta_confirmed, delta_recovered = row
            count += 1

            result.append({
                "name": country_region,
                "confirmed": confirmed,
                "deaths": deaths,
                "recovered": recovered,
                "delta_confirmed": delta_confirmed,
                "delta_recovered": delta_recovered,
                "date": last_update
            })

        return json.dumps(result)


    return app
