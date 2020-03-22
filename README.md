## Install

python -m venv venv
venv/bin/pip install -r requirements.txt
. venv/bin/activate

## (optional) Initialize the db 

export FLASK_APP=src
export FLASK_ENV=development
flask init-db

## Start flask app

export FLASK_APP=src
export FLASK_ENV=development
flask run


## Endpoints

`/import_csv`

get data from github

`/cases?country=austria`

get numbers for a country


