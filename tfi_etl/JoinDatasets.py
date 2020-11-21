import argparse


class JoinDatasets:
    def __init__(self):
        self.name = 'JoinDatasets'

    @staticmethod
    def main(spark_session, spark_conf):
        wiki_csv = spark_conf.get('PATHS', 'wiki_csv_path')
        movies_metadata_csv = spark_conf.get('PATHS', 'movies_metadata_path')

        joined_output_path = spark_conf.get('PATHS', 'joined_parquet_path')

        print('reading wiki csv file from ' + wiki_csv)
        wiki_df = spark_session.read.option('header', 'true').option("encoding", "UTF-8").csv(wiki_csv)

        print('reading movies csv from ' + movies_metadata_csv)
        movies_df = spark_session \
            .read \
            .option('header', 'true') \
            .option("encoding", "UTF-8") \
            .option("escape", "\"") \
            .option("multiLine", True) \
            .csv(movies_metadata_csv)
        # TODO check encoding - and python default encoding

        movies_df = movies_df.na.drop(subset=['title'])
        joined = movies_df.join(wiki_df, ['title'], 'left_outer')

        print('writing joined dataset to parquet at: ' + joined_output_path)
        joined.write.mode("overwrite").parquet(joined_output_path)


if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from tfi_etl.sparkscript import SparkScriptRunner

    parser = argparse.ArgumentParser()
    parser.add_argument('-config')

    args = parser.parse_args()
    config_path = str(args.config)

    join_datasets = JoinDatasets()
    script_runner = SparkScriptRunner(config_path, join_datasets)
    script_runner.run()
