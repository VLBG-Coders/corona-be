# Deployment

## 1. Deployment on Dev-Instance

**Project URL:** [TBD](http://localhost:4200/)

#### Description

All updates are currently done manually.

#### Step by step guide

Please log in to the server and follow these steps:

- `cd /var/data/websites/corona-be`
- `git pull`
- `venv/bin/pip install -r requirements.txt`
- `ps aux | grep waitress`
- `kill -9 <process-id>`
- `venv/bin/waitress-serve --call src:create_app` &

## 2. Deployment on Prod-Instance

**Project URL:** [TBD](http://localhost:4200/)

#### Description

No prod instance yet.
