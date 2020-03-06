import argparse
import sys
import re
from tabulate import tabulate
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


class GenericValidation:

    def __init__(self, s3loc, sep="\t"):
        spark = SparkSession.builder \
            .appName("GenericValidation") \
            .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")
        self.clientFile = spark.read.csv(s3loc, sep=sep)

        print("Generic File Validation \n ")
        print("===Description===")
        print("Location: %s \n") % (s3loc)
        print("File Schema")
        self.clientFile.printSchema()
        print("Example Rows")
        self.clientFile.show(n=5)
        print("===Statistics=== \nRows in File")
        print(self.clientFile.count())
        print("")
        print("CountDistinct by Column")
        self.clientFile.agg(*(f.countDistinct(f.col(c)).alias(c) for c in self.clientFile.columns)).show()


class ClusterValidationOld:

    def __init__(self, s3loc, sep="\t"):
        spark = SparkSession.builder \
            .appName("ClusterValidation") \
            .enableHiveSupport() \
            .getOrCreate()

        self.s3loc = s3loc
        self.inputFile = spark.read.csv(s3loc, sep=sep)

        self.validate_file()

    def validate_file(self):
        self.lastColPos = len(self.inputFile.columns) - 1
        print("""=== Cluster File Validation ===\nLocation: {}\n""".format(self.s3loc))

        if len(self.inputFile.columns) != 2:
            sys.exit("""Input cluster file has incorrect schema. Expected 2 columns. File has {}""".format(
                len(self.inputFile.columns)))
        else:
            print("File is valid!")

    def build_sub_aggregate(self):
        self.subagg = self.inputFile.withColumn("cookie", f.explode(f.split(self.inputFile[self.lastColPos], "\|"))) \
            .withColumn("cookieDomain", f.regexp_extract("ds", "^([^:]+)", 1)) \
            .groupBy("_c0", "cookieDomain") \
            .agg(f.count("*").alias("partnerIds"))

    def calculate_distribution(self):
        column = "partnerIDs"

        count = f.count("*").alias("ct_cluster")
        countD = f.countDistinct("_c0").alias("ct_cluster")
        percentiles = f.expr("percentile_approx({}, array(0.0,0.25,0.5,0.75,1.0), 100)".format(column)).alias("ntile")
        mean = f.round(f.avg("{}".format(column)), 1).alias("mean")

        resultsTotal = self.subagg.groupBy(f.lit("ALL").alias("cookieDomain")).agg(countD, mean, percentiles)
        resultsDomain = self.subagg.groupBy("cookieDomain").agg(count, mean, percentiles).orderBy("cookieDomain")

        self.results = resultsTotal.union(resultsDomain).collect()

        maidCounter = 0
        for r in self.results:
            if r.cookieDomain in ["aaid", "idfa"]:
                maidCounter += 1

        if maidCounter == 2:
            self.resultsMaid = self.subagg.filter("cookieDomain in ('aaid','idfa')") \
                .groupBy(f.lit("MAID ALL").alias("cookieDomain")) \
                .agg(countD, mean, percentiles) \
                .collect()

    @staticmethod
    def build_table(tableRow):
        print("""=== Cookie Domain Statistics: {} ===\n\nTotal clusters: {:,.0f}\n
        Distribution (1% margin of error)\n""".format(tableRow.cookieDomain, tableRow.ct_cluster))

        print(tabulate(
            [["Minimum", tableRow.ntile[0]],
             ["25th Percentile", tableRow.ntile[1]],
             ["Median", tableRow.ntile[2]],
             ["Mean", tableRow.mean],
             ["75th Percentile", tableRow.ntile[3]],
             ["Maximum", tableRow.ntile[4]]],
            headers=["Metric", "Partner IDs in Cluster"],
            tablefmt="presto"))
        print("\n---")
        print(" ")

    def build_distribution_report(self):
        self.build_sub_aggregate()
        self.calculate_distribution()

        for r in self.results:
            self.build_table(tableRow=r)

        if self.resultsMaid:
            for r in self.resultsMaid:
                self.build_table(tableRow=r)

        print("End of report!")


class ClusterValidation:
    def __init__(self, s3loc, sep="\t"):
        spark = SparkSession.builder \
            .appName("ClusterValidation") \
            .enableHiveSupport() \
            .getOrCreate()

        self.resultsdict = []
        self.s3loc = s3loc
        self.inputFile = spark.read.csv(s3loc, sep=sep)

        self.validate_file()

    def validate_file(self):
        self.lastColPos = len(self.inputFile.columns) - 1
        print("""=== Cluster File Validation ===\nLocation: {}\n""".format(self.s3loc))

        if re.search("^(\d+|idfa|aaid):",self.inputFile.select(self.inputFile[self.lastColPos]).head()[0]) is None:
            sys.exit("Last column does not contain clusters. Please confirm.")
        else:
            print("File is valid!")

    def calculate_distribution(self, rType="cluster"):
        if rType == "cluster":
            groupingCol= "_c0"
        elif rType == "cookie":
            groupingCol = "cookie"
        else:
            groupingCol = rType

        count = f.count("*").alias("ct")
        countD = f.countDistinct(groupingCol).alias("ct")
        percentiles = f.expr("percentile_approx(ct, array(0.0,0.25,0.5,0.75,0.85,0.90,0.95,1.0), 100)").alias("ntile")
        mean = f.round(f.avg("ct"), 1).alias("mean")

        subAgg = self.inputFile.withColumn("cookie", f.explode(f.split(self.inputFile[self.lastColPos], "\|"))) \
            .withColumn("cookieDomain", f.regexp_extract("cookie", "^([^:]+)", 1)) \
            .groupBy(groupingCol, "cookieDomain") \
            .agg(f.count("*").alias("ct"))

        resultsTotal = subAgg.groupBy(f.lit("ALL").alias("cookieDomain")).agg(countD, mean, percentiles)
        resultsDomain = subAgg.groupBy("cookieDomain").agg(count, mean, percentiles).orderBy("cookieDomain")
        results = resultsTotal.union(resultsDomain).collect()

        maidCounter = 0
        for r in results:
            if r.cookieDomain in ["aaid", "idfa"]:
                maidCounter += 1

        if maidCounter == 2:
            resultsMaid = subAgg.filter("cookieDomain in ('aaid','idfa')") \
                .groupBy(f.lit("MAID ALL").alias("cookieDomain")) \
                .agg(countD, mean, percentiles) \
                .collect()
            results.extend(resultsMaid)

        self.results_to_dict(results=results, rType=rType)

    def results_to_dict(self, results, rType):

        localResults = []

        for r in results:
            localResults.append({"cookieDomain": r.cookieDomain,
                                 rType: {"ct": r.ct,
                                         "mean": r.mean,
                                         "min": r.ntile[0],
                                         "max": r.ntile[7],
                                         "ntile": {"25": r.ntile[1],
                                                   "50": r.ntile[2],
                                                   "75": r.ntile[3],
                                                   "85": r.ntile[4],
                                                   "90": r.ntile[5],
                                                   "95": r.ntile[6]}}})
        if len(self.resultsdict) == 0:
            self.resultsdict = localResults
        else:
            for r in self.resultsdict:
                for s in localResults:
                    if s["cookieDomain"] == r["cookieDomain"]:
                        r.update(s)

    @staticmethod
    def build_table(resultRow):
        r = resultRow

        print("""=== Cookie Domain Statistics: {} ===\n\nTotal Clusters: {:,.0f}\nTotal Partner IDs (PIDs): {:,.0f}\n
            Distribution (1% margin of error)\n""".format(r["cookieDomain"], r["cluster"]["ct"], r["cookie"]["ct"]))
        print(tabulate(
            [["Minimum", r["cluster"]["min"], r["cookie"]["min"]],
             ["25th Percentile", r["cluster"]["ntile"]["25"], r["cookie"]["ntile"]["25"]],
             ["Median", r["cluster"]["ntile"]["50"], r["cookie"]["ntile"]["50"]],
             ["Mean", r["cluster"]["mean"], r["cookie"]["mean"]],
             ["75th Percentile", r["cluster"]["ntile"]["75"], r["cookie"]["ntile"]["75"]],
             ["85th Percentile", r["cluster"]["ntile"]["85"], r["cookie"]["ntile"]["85"]],
             ["90th Percentile", r["cluster"]["ntile"]["90"], r["cookie"]["ntile"]["90"]],
             ["95th Percentile", r["cluster"]["ntile"]["95"], r["cookie"]["ntile"]["95"]],
             ["Maximum", r["cluster"]["max"], r["cookie"]["max"]]],
            headers=["Metric", "PIDs Per Cluster", "Clusters Per PID"],
            tablefmt="presto"))
        print("\n---")
        print(" ")

    def build_distribution_report(self):
        self.calculate_distribution(rType="cluster")
        self.calculate_distribution(rType="cookie")

        for r in self.resultsdict:
            self.build_table(resultRow=r)


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
        run.build_distribution_report()
        print("End of Report!")
    elif test == "generic":
        run = GenericValidation(s3loc=s3loc, sep=sep)
        print("End of Report!")
    else:
        sys.ext("No test specified. Exiting!")