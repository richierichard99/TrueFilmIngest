import argparse


class LoadPostgres:
    def __init__(self):
        self.name = 'LoadPostgres'

    @staticmethod
    def main(spark, config):

        sql_input_path = config.get('PATHS', 'sql_input_path')
        rows_to_load = int(config.get('DB_SETTINGS', 'rows'))
        sql_input_df = spark.read.parquet(sql_input_path).limit(rows_to_load)

        tablename = config.get('DB_SETTINGS', 'tablename')
        db_url = config.get('DB_SETTINGS', 'url')
        print('loading to postgres db: ' + db_url)
        print('table name: ' + tablename)

        properties = {
            'user': config.get('DB_SETTINGS', 'username'),
            'password': config.get('DB_SETTINGS', 'password')
        }

        sql_input_df\
            .write\
            .option('driver', 'org.postgresql.Driver')\
            .jdbc(db_url, tablename, 'overwrite', properties)


if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from tfi_etl.sparkscript import SparkScriptRunner

    parser = argparse.ArgumentParser()
    parser.add_argument('-config')
    args = parser.parse_args()
    config_path = str(args.config)

    load_postgres = LoadPostgres()
    script_runner = SparkScriptRunner(config_path, load_postgres)
    script_runner.run()
