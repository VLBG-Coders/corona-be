# Installation Guide

## Requirements

- [python3](https://www.python.org/download/releases/3.0/)

## Installation steps

### 1. Clone the repository

```
git clone git@github.com:VLBG-Coders/corona-be.git
cd corona-be
```

### 2. Install the project

```
python -m venv venv

venv/bin/pip install -r requirements.txt

. venv/bin/activate
```

### 3. Initialize the database

```
export FLASK_APP=src

export FLASK_ENV=development

venv/bin/flask init-db
```

**Import Data:**
- http://127.0.0.1:5000/import_countries
- http://127.0.0.1:5000/import_covid19

### 4. Run the project

*(set the parameters once)*
```
export FLASK_APP=src

export FLASK_ENV=development
```

*run the app*
```
venv/bin/flask run
```

The project is now up and running on `http://127.0.0.1:5000/`.

Please read the [development guide](development.md) for more information.
