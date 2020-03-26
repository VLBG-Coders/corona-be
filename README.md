## Install

python -m venv venv

venv/bin/pip install -r requirements.txt

. venv/bin/activate

## (optional) Initialize the db 

export FLASK_APP=src
export FLASK_ENV=development
venv/bin/flask init-db

## Start flask app

export FLASK_APP=src
export FLASK_ENV=development
venv/bin/flask run


## Endpoints

`/import_csv`

import data from github

`/cases-by-country?country=austria`

get numbers for a country

`/cases-by-countries`

Get current number for each country and their delta

`/cases-total-days`

Total numbers per days

`/countries`

list of countries
