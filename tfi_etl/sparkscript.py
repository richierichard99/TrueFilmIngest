from pyspark.sql import SparkSession

"""
    Main Class to run pyspark scripts
    -   config: config options needed to run scripts
    -   spark_script: script class to run. must contain a name value and main method. 

    e.g. class ExampleScript:
                def __init__(self):
                    self.name = 'ExampleScript'

                def main(spark, config):
                    *** SCRIPT TO RUN **** 

"""


class SparkScriptRunner:

    def __init__(self, config, spark_script):
        self.spark_script = spark_script
        self.config = config

    def run(self):
        print('SparkScriptRunner: ' + self.spark_script.name)
        spark = SparkSession.builder.appName(self.spark_script.name).getOrCreate()
        self.spark_script.main(spark, self.config)
