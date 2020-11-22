# TrueFilmIngest

repository to load movies and wikipedia information to PostgreSQL database

## Requirements
* python 2.7
* Spark 2.3 - installation instructions, windows: https://phoenixnap.com/kb/install-spark-on-windows-10 linux: https://phoenixnap.com/kb/install-spark-on-ubuntu
* Docker: used to dockerise a standard PostgreSQL instance 
* 7zip

## Repository setup

run setup scripts:
```
windows:
.\setup.cmd 
linux:
./setup.sh
```

this will download the following pyhton packages and external .jar files:
pyhton:
* pyspark - Spark for python. this was chosen to easily write scalable data pipelines. the repository is set up to run on a local version of spark, 
but this could be scaled to a full large spark cluster with limited changes to the codebase. 
* requests - Used for the downloader script that downloads jar files needed for spark classpath.
* configparser - package to parse values from `.ini` files.
* pyYaml - used to convert dictionary strings into yaml objects to extract data (see `tfi_etl/CreateSqlInput.py`)

other (`.jar`):
* databricks spark-xml - spark xml parser to read xml files into spark dataframe
* postgres jbdc driver - driver used to connect to postgres db from spark

## Data Preparation

download data to your environment and extract to a location of choice (use `scripts/config.ini` as a guide for how to structure):
  * https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz
  * https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7#movies_metadata.csv

use 7zip to extract wiki abstract file. eg. `7z -e ...`

Update `scripts/config.ini` to match the data root of the downloaded files, edit `wiki_xml_path` and `movies_metadata_path` 
to point at the en-wiki xml and movies metadata csv filepaths respectively.

## Postgres Setup

### Docker

if there is no existing postgres instance running, there is an included docker compose file to create a dockerised postgres instance. run:
```
docker-compose -f postgres/stack.yml up
```
this will create a postgres db instance with default credentials (postgres:postgres) with a tunnel set up to `localhost:5432`. the scripts are configured to connect to this by default. (see `scripts/config.ini` under `[DB_SETTINGS]`

to change usernames and passwords edit the `stack.yml` with the desired credentials.
you will also have to update the corresponding username,password config keys under `[DB_SETTINGS]` in `scripts/config.ini` to match the new credentials.

this will also instantiate a ui admin running on `localhost:8080` to view tables and data.

if those ports are already in user they can be configured in `/config/stack.yml`

### Existing DB

if there is already an existing postgres db, the scripts can be configured to connect and load to this. edit the database settings under `[DB_SETTINGS]` in `scritps/config.ini`
to match the database to you are loading to.

## Run ETL

naviage to the `scripts` directory, ensure all values in `config.ini` are correct for your environment (paths and db).
for windows users:
```
.\runETLAll.cmd
```
for linux users:
```
./runETLAll.sh
```

## Test for correctness

* Downsides of spark include that is difficult to test jobs - future work to this repository would include setting up a small spark testing environment (potentially dockerised) to run the etl scripts on test data and check expected results.
* Verify the output of each job - do row counts make sense (e.g. the output of `JoinDatasets.py` should be the same size as the `movies_metadata.csv`), are all the columns present and visually inspect the data. (can use a `spark-shell` to view the contents of a dataframe)
checks could also include checking that titles are unique (`df.groupBy("title").count.orderBy(desc("count")).show(false)`).
* Check the sql table - visual inspection using the admin tool (are the correct columns present, does the data look correct), or from the command line `SELECT * FROM "truefilm" LIMIT 50 OFFSET 100`




