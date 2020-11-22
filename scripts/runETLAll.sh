#!/usr/bin/env bash
./runSparkSubmit.sh ImportXml.py config.ini
./runSparkSubmit.sh JoinDatasets.py config.ini
./runSparkSubmit.sh CreateSqlInput.py config.ini
./runSparkSubmit.sh LoadPostgres.py config.ini
