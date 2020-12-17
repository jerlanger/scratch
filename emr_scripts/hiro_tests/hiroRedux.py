import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


class defaultTest(object):

    def __init__(self, clientFile, ticket, hem="md5"):

        self.clientFile = clientFile
        self.s3Location = "s3n://ds-emr-storage/jira/seid/%s" % (ticket)
        self.useCovertedFile = False

        print("""File to be analyzed: %s \n""" % (self.clientFile))

        if (hem == "sha1") | (hem == "sha2"):
            self.hem = hem
            self.hem_transformation()
            self.convertedClientFile = """%s/tmpMd5/""" % (self.s3Location)
            self.useCovertedFile = True
        elif hem != "md5":
            sys.exit("""Invalid type given. Expected valid options: [md5,sha1,sha2]""")

    def stat_test(self):

        spark = SparkSession.builder \
            .appName("stat_test") \
            .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")

        client = spark.read.csv(self.clientFile) \
            .withColumnRenamed("_c0", "hem") \
            .coalesce(10000)

        print("--- STAT TEST ---\n"
              "Date of LiveIntent's most recent dataset: 20200415 \n")

        sellableInit = spark.sql("""SELECT piiidentifier as hem, cookie_domain_p as cDomain, COUNT(*) pairs, 
                        COUNT(CASE WHEN region_p = 'US' THEN region_p END) us_pairs 
                        FROM auto_sellable.sellable_pair_with_attributes
                        WHERE date_p = '20200415' GROUP BY 1,2""")

        print("=== Contents of Client HEM File ===\n"
              "This test verifies the content of the original client file by searching for standard\n"
              "input formats (MD5,SH1,SH2). However, please note that it is only possible to evaluate\n"
              "if the format is correct. It is not possible to evaluate if the input to create\n"
              "the hem was correct.\n")

        client.withColumn("hType", F.when(F.col("hem").rlike("^[a-f0-9]{32}$"), "md5") \
                          .when(F.col("hem").rlike("^[a-f0-9]{40}$"), "sh1") \
                          .when(F.col("hem").rlike("^[a-f0-9]{64}$"), "sh2") \
                          .otherwise("invalid")) \
            .groupBy(F.col("hType")) \
            .agg(F.count(F.col("hem")).alias("ct_hem"),
                 F.countDistinct(F.col("hem")).alias("ctD_hem")) \
            .show()

        if self.useCovertedFile:
            print("If you converted the file, the following is the breakdown of the generated\n"
                  "conversion file. THIS FILE WILL BE USED FOR THE STAT TEST.\n")

            clientConverted = spark.read.csv(self.convertedClientFile) \
                .withColumnRenamed("_c0", "hem")

            clientConverted.withColumn("hType", F.when(F.col("hem").rlike("^[a-f0-9]{32}$"), "md5") \
                                       .when(F.col("hem").rlike("^[a-f0-9]{40}$"), "sh1") \
                                       .when(F.col("hem").rlike("^[a-f0-9]{64}$"), "sh2") \
                                       .otherwise("invalid")) \
                .groupBy(F.col("hType")) \
                .agg(F.count(F.col("hem")).alias("ct_hem"),
                     F.countDistinct(F.col("hem")).alias("ctD_hem")) \
                .show()

            sellableHash = sellableInit.join(clientConverted, on="hem", how="inner")
        else:
            sellableHash = sellableInit.join(client, on="hem", how="inner")

        sellableHash.cache()

        print("=== Number of Matched Hashes to Sellable Dataset ===\n"
              "This test informs the hem overlap between the client and LiveIntent's most\n"
              "recent sellable dataset. The last column narrows the matches to only hems that\n"
              "are located in the USA.\n")

        sellableHash \
            .agg(F.countDistinct(sellableHash.hem).alias("matchedHems"),
                 F.countDistinct(F.when(sellableHash.us_pairs > 0, sellableHash.hem)).alias(
                     """matchedHems (US only)""")) \
            .show()

        print("=== Cookie Domain Breakdown  ===\n"
              "The number of client hems and potential pairs for each cookie domain\n"
              "located in the most recent sellable dataset. Sorted by number of pairs.\n"
              "Only a maximum of 50 domains will be shown.\n")

        domainNames = spark.sql("""SELECT pub_or_app_id as id, name as cDomainName
                                    FROM default.sellable_domain_names""")

        sellableHash \
            .join(domainNames, sellableHash.cDomain == domainNames.id, how="left") \
            .groupBy(F.col("cDomain"), F.col("cDomainName")) \
            .agg(
            F.countDistinct(F.when(sellableHash.us_pairs > 0, sellableHash.hem)).alias("""matchedHems (US only)"""),
            F.sum("us_pairs").alias("Pairs (US only)")) \
            .orderBy("Pairs (US only)", ascending=False) \
            .show(50)

        sellableHash.unpersist()

    def hem_transformation(self):

        spark = SparkSession.builder \
            .appName("hem_lookup") \
            .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("auto_mappings")

        maxHemLookupDate = spark.sql("""SELECT MAX(date_p) FROM auto_mappings.consolidated_email_hash""") \
            .collect()[0][0]

        joinCol = self.hem.replace("a", "")

        print("--- HEM TRANSFORMATION ---\n"
              "Converting %s to md5....\n"
              "Using consolidated email hash dataset from %s\n" % (self.hem, maxHemLookupDate))

        client = spark.read.csv(self.clientFile) \
            .withColumnRenamed("_c0", joinCol)

        hemLookup = spark.sql("""SELECT md5, %s 
                        FROM auto_mappings.consolidated_email_hash 
                        WHERE date_p = '%s'""" % (joinCol, maxHemLookupDate))

        hemLookup.join(client, on=joinCol, how="inner") \
            .select(hemLookup.md5) \
            .distinct() \
            .write \
            .csv("""%s/tmpMd5/""" % (self.s3Location), mode="overwrite")

        print("""Conversion complete!\n""")

#s3loc = ["s3://bidswitch-partner-liveintent-com/onboarding/input/Audience1.txt","s3://bidswitch-partner-liveintent-com/onboarding/input/Audience2.txt","s3://bidswitch-partner-liveintent-com/onboarding/input/Audience3.txt"]

s3loc = ["s3://bidswitch-partner-liveintent-com/onboarding/input/Audience*"]

for r in s3loc:
    clientTest = defaultTest(clientFile=r, ticket="seid454", hem="md5")
    clientTest.stat_test()