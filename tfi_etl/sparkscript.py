from pyspark.sql import SparkSession
from ConfigParser import ConfigParser
import argparse

"""
    Main Class to run pyspark scripts
    -   config_path: path to .ini file with config options needed to run scripts
    -   spark_script: script class to run. must contain a name value and main method. 

    e.g. class ExampleScript:
                def __init__(self):
                    self.name = 'ExampleScript'

                def main(spark, config):
                    *** SCRIPT TO RUN **** 

"""


class SparkScriptRunner:

    def __init__(self, config_path, spark_script):
        self.spark_script = spark_script
        self.config_path = config_path

    @staticmethod
    def read_config(config_path):
        config = ConfigParser()
        config.readfp(file(config_path))
        return config

    def run(self):
        print('SparkScriptRunner: ' + self.spark_script.name)
        config = self.read_config(self.config_path)
        spark = SparkSession.builder.appName(self.spark_script.name).getOrCreate()
        self.spark_script.main(spark, config)
