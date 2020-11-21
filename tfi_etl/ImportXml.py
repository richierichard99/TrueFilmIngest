from pyspark.sql.functions import regexp_replace
from ConfigParser import ConfigParser


class ImportXML:
    def __init__(self):
        self.name = 'ImportXml'

    @staticmethod
    def read_xml(spark_session, input_xml, row_tag):
        print("reading xml file from " + input_xml)
        df = spark_session \
            .read \
            .format('com.databricks.spark.xml') \
            .option('rowTag', row_tag) \
            .load(input_xml)
        return df

    @staticmethod
    def main(spark_session, spark_conf):
        wiki_xml_path = spark_conf.get('PATHS', 'wiki_xml_path')
        wiki_df = ImportXML.read_xml(spark_session, wiki_xml_path, row_tag='doc')
        # TODO: csv path in a folder conventions file?
        wiki_csv_path = spark_conf.get('PATHS', 'wiki_csv_path')
        wiki_df = wiki_df.select(['title', 'abstract', 'url'])
        wiki_df = wiki_df.withColumn('title', regexp_replace('title', 'Wikipedia: ', ''))

        print('writing converted csv file to ' + wiki_csv_path)
        wiki_df.write.option('header', 'true').mode('overwrite').csv(wiki_csv_path)


if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from tfi_etl.sparkscript import SparkScriptRunner

    config_path = 'C:/myDev/trueLayer/repository/TrueFilmIngest/scripts/config.ini'
    config = ConfigParser()
    config.readfp(file(config_path))
    xml_import = ImportXML()

    script_runner = SparkScriptRunner(config, xml_import)
    script_runner.run()
