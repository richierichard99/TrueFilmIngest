from pyspark.sql import SparkSession


class ImportXML:
    @staticmethod
    def read_xml(spark_session, input_xml, row_tag):
        print("reading xml file from " + input_xml)
        df = spark_session \
            .read \
            .format('com.databricks.spark.xml') \
            .option('rowTag', row_tag) \
            .load(input_xml)
        return df


if __name__ == '__main__':

    spark = SparkSession.builder.appName('ImportXml').getOrCreate()

    # TODO: Read in from config
    wiki_xml_path = 'C:/myDev/trueLayer/data/enwiki-latest-abstract/enwiki-latest-abstract.xml'
    wiki_df = ImportXML.read_xml(spark, wiki_xml_path, row_tag='doc')

    wiki_csv_path = wiki_xml_path.replace('.xml', '.csv')

    wiki_df.select(['title', 'abstract', 'url']).write.mode('overwrite').csv(wiki_csv_path)
