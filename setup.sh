#!/bin/bash
# python package installation
pip install -r requirements.txt

# spark dependencies
python downloader.py -req maven_requirements.txt

# postgres drivers
python downloader.py -url https://jdbc.postgresql.org/download/ -req postgres_requirements.txt