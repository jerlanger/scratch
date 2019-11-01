
#coding: utf-8

def validation_test(FileLocation):
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f

    spark = SparkSession.builder \
                        .appName("FileStats") \
                        .config("hive.metastore.client.factory.class",
                               "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
                        .enableHiveSupport() \
                        .getOrCreate()

    spark.catalog.setCurrentDatabase("default")
    ClientFile = spark.read.csv(FileLocation, sep="\t")
    
    print(r"""
                                       ._ o o
                                       \_`-)|_
                                    ,""       \  
                                  ,"  ## |   0 0.    THAT'S WHAT'S UP!
                                ," ##   ,-\__    `.  LET'S ANALYZE!
                              ,"       /     `--._;) YEAH!
                            ,"     ## /
                          ,"   ##    /
                    """)
    print("")
    print("===Description===")
    print("Location: %s \n") %(FileLocation)
    print("File Schema")
    ClientFile.printSchema()
    print("Example Rows")
    ClientFile.show(5)
    print("===Statistics=== \nRows in File")
    print(ClientFile.count())
    print("")
    print("CountDistinct by Column")
    ClientFile.agg(*(f.countDistinct(f.col(c)).alias(c) for c in ClientFile.columns)).show()

import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--s3loc", help="The full s3 folder location beginning with 's3://'")
args = parser.parse_args()

if args.s3loc:
    FileLocation = args.s3loc
else:
    sys.exit("No File Location Provided")

validation_test(FileLocation)
