import os
import sqlite3
import json
import csv
import requests
from . import db


from flask import Flask, g, request

CSV_URL="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/web-data/data/cases_time.csv"
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


    def read_and_import_csv(data):
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
                query = "DELETE FROM cases_time"
                db.get_db().execute(query)
                continue
            country_region, last_update, confirmed, deaths, recovered, active, delta_confirmed, delta_recovered = row

            result.append(row)

            placeholder = ["?" for i in range(len(header))]
            query = f'INSERT INTO cases_time ({",".join(header).lower()}) VALUES ({",".join(placeholder)})'
            db.get_db().execute(query, row)
            db.get_db().commit()

        return json.dumps(result)


    # testing only
    @app.route('/import_test')
    def import_test():
        with open('cases_time.csv') as csvfile:
            data = read_and_import_csv(csvfile)
        return data


    @app.route('/import_csv')
    def import_csv():
        download = requests.get(CSV_URL)
        decoded_content = download.content.decode('utf-8')
        data = decoded_content.split("\n")
        result = read_and_import_csv(data)
        return result

    @app.route('/cases')
    def cases():
        country = request.args.get("country")
        
        if not country:
            return "please provide a country", 400

        cursor = db.get_db().cursor()
        query = f"SELECT * FROM cases_time WHERE LOWER(country_region) = '{country.lower()}' ORDER BY last_update ASC"
        cursor.execute(query)

        result = []
        for row in cursor.fetchall():
            result.append(dict(zip(row.keys(), row)))

        return json.dumps(result)


    return app
