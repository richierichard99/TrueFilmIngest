import argparse
import json
from pyspark.sql.functions import udf, when, year


class CreateSqlInput:
    def __init__(self):
        self.name = 'CalculateStats'

    @staticmethod
    @udf
    def extract_production(json_string):
        try:
            production_array = json.loads(json_string.replace("'", "\""))
            parsed_production = []
            for production in production_array:
                parsed_production.append(production['name'])
        except ValueError:
            parsed_production = []
        return parsed_production

    @staticmethod
    def main(spark, config):
        joined_parquet_path = config.get('PATHS', 'joined_parquet_path')
        sql_input_path = config.get('PATHS', 'sql_input_path')
        joined_df = spark.read.parquet(joined_parquet_path)

        joined_df = joined_df.withColumn('production_companies',
                                         CreateSqlInput.extract_production('production_companies'))

        joined_df = joined_df.withColumn('ratio',
                                         when(joined_df['revenue'] != 0, joined_df['budget']/joined_df['revenue'])
                                         .otherwise(0.0))

        joined_df = joined_df.withColumn('year', year('release_date'))

        joined_df = joined_df.orderBy('ratio', ascending=False)
        joined_df.select('title', 'production_companies', 'budget', 'revenue', 'ratio', 'year').show(5, False)

        joined_df.select(['title',
                          'budget',
                          'year',
                          'revenue',
                          'vote_average',
                          'ratio',
                          'production_companies',
                          'url',
                          'abstract']).write.mode('overwrite').parquet(sql_input_path)


if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from tfi_etl.sparkscript import SparkScriptRunner

    parser = argparse.ArgumentParser()
    parser.add_argument('-config')

    args = parser.parse_args()
    config_path = str(args.config)

    calculate_stats = CreateSqlInput()
    script_runner = SparkScriptRunner(config_path, calculate_stats)
    script_runner.run()
