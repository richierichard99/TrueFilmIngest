#!/bin/bash
# python package installation
pip install -r requirements.txt

# spark dependencies
python setup.py -req maven_requirements.txt

# postgres drivers
python setup.py -url https://jdbc.postgresql.org/download/ -req postgres_requirements.txt