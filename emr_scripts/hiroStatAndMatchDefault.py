class defaultTest(object):
    def __init__(self,clientFile,ticket,hem="md5"):
        import sys

        self.clientFile = clientFile
        self.s3Location = "s3n://ds-emr-storage/jira/seid/%s" % (ticket)
        self.useCovertedFile = False

        print("""File to be analyzed: %s \n""" %(self.clientFile))

        if (hem == "sha1") | (hem == "sha2"):

            self.hem = hem
            self.hem_transformation()
            self.convertedClientFile = """%s/tmpMd5/""" %(self.s3Location)
            self.useCovertedFile = True

        elif hem != "md5":
            sys.exit("""Invalid type given. Expected valid options: [md5,sha1,sha2]""")

    def stat_test(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import countDistinct, col, when, isnull, count, sum

        spark = SparkSession.builder \
            .appName("stat_test") \
            .config("hive.metastore.client.factory.class", 
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")

        maxSellableDate = spark.sql("""SELECT MAX(date_p) FROM auto_sellable.sellable_pair""") \
                                 .collect()[0][0]
        
        client = spark.read.csv(self.clientFile) \
            .withColumnRenamed("_c0","hem")

        print("""--- STAT TEST --- 
        Date of LiveIntent's most recent dataset: %s \n""" %(maxSellableDate))

        sellableInit = spark.sql("""SELECT piiidentifier as hem, cookie_domain_p as cDomain, COUNT(*) pairs, 
                        COUNT(CASE WHEN region_p = 'US' THEN region_p END) us_pairs 
                        FROM auto_sellable.sellable_pair 
                        WHERE date_p = '%s' GROU        P BY 1,2""" % (maxSellableDate))

        print(""" === Contents of Client HEM File ===
        This test verifies the content of the original client file by searching for standard
        input formats (MD5,SH1,SH2). However, please note that it is only possible to evaluate
        if the format is correct. It is not possible to evaluate if the input to create 
        the hem was correct.\n""")

        client.withColumn("hType",when(col("hem").rlike("^[a-f0-9]{32}$"),"md5") \
                    .when(col("hem").rlike("^[a-f0-9]{40}$"),"sh1") \
                    .when(col("hem").rlike("^[a-f0-9]{64}$"),"sh2") \
                          .otherwise("invalid")) \
            .groupBy(col("hType")) \
            .agg(count(col("hem")).alias("ct_hem"),
                 countDistinct(col("hem")).alias("ctD_hem")) \
            .show()

        if self.useCovertedFile:
            print("""If you converted the file, the following is the breakdown of the generated
            conversion file. THIS FILE WILL BE USED FOR THE STAT TEST. \n""")

            clientConverted = spark.read.csv(self.convertedClientFile) \
                                    .withColumnRenamed("_c0","hem")

            clientConverted.withColumn("hType", when(col("hem").rlike("^[a-f0-9]{32}$"), "md5") \
                              .when(col("hem").rlike("^[a-f0-9]{40}$"), "sh1") \
                              .when(col("hem").rlike("^[a-f0-9]{64}$"), "sh2") \
                              .otherwise("invalid")) \
                .groupBy(col("hType")) \
                .agg(count(col("hem")).alias("ct_hem"),
                     countDistinct(col("hem")).alias("ctD_hem")) \
                .show()

            sellableHash = sellableInit.join(clientConverted, on="hem", how="inner")
        else:
            sellableHash = sellableInit.join(client, on="hem", how="inner")

        sellableHash.cache()

        print("""=== Number of Matched Hashes to Sellable Dataset ===
        This test informs the hem overlap between the client and LiveIntent's most
        recent sellable dataset. The last column narrows the matches to only hems that
        are located in the USA.\n""")

        sellableHash \
            .agg(countDistinct(sellableHash.hem).alias("matchedHems"),
                 countDistinct(when(sellableHash.us_pairs > 0, sellableHash.hem)).alias("""matchedHems (US only)""")) \
            .show()

        print("""=== Cookie Domain Breakdown  ===
        The number of client hems and potential pairs for each cookie domain
        located in the most recent sellable dataset. Sorted by number of pairs. 
        Only a maximum of 50 domains will be shown.\n""")

        domainNames = spark.sql("""SELECT pub_or_app_id as id, name as cDomainName
                                    FROM default.sellable_domain_names""")

        sellableHash \
            .join(domainNames, sellableHash.cDomain == domainNames.id, how="left") \
            .groupBy(col("cDomain"),col("cDomainName")) \
            .agg(countDistinct(when(sellableHash.us_pairs > 0, sellableHash.hem)).alias("""matchedHems (US only)"""),
                 sum("us_pairs").alias("Pairs (US only)")) \
            .orderBy("Pairs (US only)", ascending=False) \
            .show(50)
        
        sellableHash.unpersist()

    def match_test(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import isnull

        spark = SparkSession.builder \
            .appName("match_test") \
            .config("hive.metastore.client.factory.class", 
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")

        maxSellableDate = spark.sql("""SELECT MAX(date_p) FROM auto_sellable.sellable_pair""") \
                                 .collect()[0][0]

        maxLididDate = spark.sql("""SELECT MAX(date_p) FROM auto_dmps.all_features_mapping_pair""") \
                                 .collect()[0][0]

        # Create temp tables #

        if self.useCovertedFile:
            client = spark.read.csv(self.convertedClientFile) \
                                    .withColumnRenamed("_c0","hem")
        else:
            client = spark.read.csv(self.client_file) \
                                    .withColumnRenamed("_c0","hem")
        
        lidid = spark.sql("""SELECT hash as hem FROM auto_dmps.all_features_mapping_pair 
                     WHERE identifier RLIKE '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$' 
                     AND date_p = '%s' GROUP BY 1"""  % (maxLididDate))
        
        maid = spark.sql("""SELECT piiidentifier as hem FROM auto_sellable.sellable_pair 
                    WHERE cookie_domain_p IN ('aaid','idfa') 
                    AND date_p = '%s' GROUP BY 1""" % (maxSellableDate))

        # Generate match file #

        match_floc = "%s/match_test/standard_match/L%sM%s/" \
        % (self.s3Location, maxLididDate, maxSellableDate)
        
        client \
            .join(lidid,client.hem == lidid.hem, "left") \
            .join(maid,client.hem == maid.hem, "left") \
            .select(client.hem, lidid.hem.isNotNull().alias("has_lidid"), maid.hem.isNotNull().alias("has_maid")) \
            .distinct() \
            .write \
            .parquet(match_floc)

    def hem_transformation(self):
        from pyspark.sql import SparkSession
        import sys

        spark = SparkSession.builder \
            .appName("hem_lookup") \
            .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("auto_mappings")

        maxHemLookupDate = spark.sql("""SELECT MAX(date_p) FROM auto_mappings.consolidated_email_hash""") \
                                .collect()[0][0]

        if self.hem == "sha1":
            joinCol = "sh1"
        elif self.hem == "sha2":
            joinCol = "sh2"

        print("""--- HEM TRANSFORMATION ---
        Converting %s to md5....
        Using consolidated email hash from %s""" % (self.hem,maxHemLookupDate))

        client = spark.read.csv(self.clientFile) \
            .withColumnRenamed("_c0",joinCol)

        hemLookup = spark.sql("""SELECT md5, %s 
                        FROM auto_mappings.consolidated_email_hash 
                        WHERE date_p = '%s'""" %(joinCol,maxHemLookupDate))

        hemLookup.join(client, join=joinCol, how="inner") \
                .select(hemLookup.md5) \
                .distinct() \
                .write \
                .csv("""%s/tmpMd5/""" %(self.s3Location), mode="overwrite")