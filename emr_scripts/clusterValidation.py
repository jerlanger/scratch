import argparse
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from tabulate import tabulate


class GenericValidation:

    def __init__(self, s3loc, sep="\t"):
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as f

        spark = SparkSession.builder \
            .appName("GenericValidation") \
            .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")
        clientFile = spark.read.csv(s3loc, sep=sep)

        print("Generic File Validation \n ")
        print("===Description===")
        print("Location: %s \n") % (s3loc)
        print("File Schema")
        clientFile.printSchema()
        print("Example Rows")
        clientFile.show(n=5, truncate=False)
        print("===Statistics=== \nRows in File")
        print(clientFile.count())
        print("")
        print("CountDistinct by Column")
        clientFile.agg(*(f.countDistinct(f.col(c)).alias(c) for c in ClientFile.columns)).show()


class ClusterValidation:

    def __init__(self, s3loc, sep="\t"):

        spark = SparkSession.builder \
            .appName("ClusterValidation") \
            .enableHiveSupport() \
            .getOrCreate()

        self.inputFile = spark.read.csv(s3loc, sep=sep)

        if len(inputFile.columns) != 2:
            sys.exit("""Input cluster file has incorrect schema. Expected 2 columns. File has {}""".format(
                len(self.inputFile.columns)))
        else:
            print("""Location: {}""".format(s3loc))

        self.get_domains()

    def get_domains(self):

        inputDomains = self.inputFile.withColumn("dSplit", f.explode(f.split(f.col("_c1"), "\|"))) \
            .withColumn("cookieDomain", f.regexp_extract(f.col("dSplit"), "^([^:]+)", 1)) \
            .select("cookieDomain") \
            .distinct() \
            .orderBy("cookieDomain") \
            .collect()

        if len(inputDomains) > 1:
            self.domain_distribution()

    # to do: come up with solution to do aaid + idfa counts, preferably within the existin framework #

        maidCounter = 0

        for r in inputDomains:
            if r.cookieDomain in ["aaid", "idfa"]:
                maidCounter += 1

            self.domain_distribution(cookieDomain=r.cookieDomain)

    def domain_distribution(self, cookieDomain=None):

        if cookieDomain is None:
            domainClusters = self.inputFile.withColumn("partnerIds", f.size(f.split(f.col("_c1"), "\|")))
            cookieDomain = "ALL"
        else:
            domainClusters = self.inputFile.filter(f.col("_c1").like("%{}:%".format(cookieDomain))) \
                .withColumn("partnerIds", f.size(f.split(f.col("_c1"), "{}:".format(cookieDomain))) - 1)

        dcCount = domainClusters.count()
        dcAvg = domainClusters.agg(f.avg("partnerIds")).collect()[0][0]
        dcQuant = domainClusters.approxQuantile("partnerIds", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

        print("""=== Cookie Domain Statistics: {} ===\nTotal clusters: {:,.0f}\n
        Distribution (5% margin of error)\n""".format(cookieDomain, dcCount))

        print(tabulate(
            [["Minimum", dcQuant[0]], ["25th Percentile", dcQuant[1]], ["Median", dcQuant[2]], ["Mean", round(dcAvg)],
             ["75th Percentile", dcQuant[3]], ["Maximum", dcQuant[4]]],
            headers=["Metric", "Partner IDs in Cluster"],
            tablefmt="presto"))
        print("\n---")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3loc", help="The full s3 location beginning with 's3://'")
    parser.add_argument("--sep", default="\t", help="Delimiter for input file.")
    parser.add_argument("--test", default="generic", help="Desired test type.")
    args = parser.parse_args()

    if args.s3loc:
        s3loc = args.s3loc
    else:
        sys.exit("No file location provided. Exiting!")

    sep = args.sep
    test = args.test

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

    if test == "cluster-distribution":
        run = ClusterValidation(s3loc=s3loc, sep=sep)
    elif test == "generic":
        run = GenericValidation(s3loc=s3loc, sep=sep)
    else:
        sys.ext("No test specified. Exiting!")
