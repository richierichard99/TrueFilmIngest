#!/usr/bin/env bash
# set to run on local mode - master needs configuring for any other run mode
spark-submit \
    --master local \
    --driver-memory 2g \
    --jars ../dependencies/spark-xml_2.11-0.10.0.jar,../dependencies/postgresql-42.2.18.jar \
    ../tfi_etl/$1 -config $2
