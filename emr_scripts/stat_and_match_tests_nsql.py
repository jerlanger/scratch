class seid(object):        
    def __init__(self,client,ticket):
        from pyspark.sql import SparkSession
        
        self.client = client
        self.ticket = ticket
        self.base_floc = "s3n://ds-emr-storage/jira/seid/%s_%s" % (ticket, client)
        self.client_file = "%s/match_file/" % (self.base_floc)
        self.client_table = "match_tests.%s_%s" % (ticket, client)
        
        spark = SparkSession.builder \
            .appName("init") \
            .config("hive.metastore.client.factory.class", 
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")
        
        self.max_sellable_date = spark.sql("""SELECT MAX(date_p) FROM auto_sellable.sellable_pair""") \
                                 .collect()[0][0]
        
        self.max_agg_date = spark.sql("""SELECT MAX(date_p) FROM auto_dmps.all_features_mapping_pair""") \
                                 .collect()[0][0]

    def stat_test(self):
        #from pyspark.sql import SparkSession
        #from pyspark.sql.functions import countDistinct, col, when, isnull, count

        spark = SparkSession.builder \
            .appName("stat_test") \
            .config("hive.metastore.client.factory.class", 
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")

        # Create stat tables #
        
        
        client = spark.read.csv(self.client_table) \
            .withColumnRenamed("_c0","hem")
        
        sell_hash_q = """SELECT piiidentifier as hem, region_p, cookie_domain_p, cookie 
                         FROM auto_sellable.sellable_pair 
                         WHERE date_p = '%s' GROUP BY 1,2,3,4""" % (self.max_sellable_date)
        
        sellable_hash = spark.sql(sell_hash_q)         

        # Quality check #

        qc_floc = "%s/stat_test/agg/encryption_validity/" % (self.base_floc)

        client.withColumn("hType",when(col("hem").rlike("^[a-f0-9]{32}$"),"md5") \
                    .when(col("hem").rlike("^[a-f0-9]{40}$"),"sh1") \
                    .when(col("hem").rlike("^[a-f0-9]{64}$"),"sh2")) \
            .groupBy(col("hType")) \
            .agg(count(col("hem")).alias("count_hem"), \
                 countDistinct(col("hem")).alias("dCount_hem")) \
            .coalesce(1) \
            .write \
            .csv(qc_floc, header=True, mode="overwrite")

        # Matched hashes #
        
        mh_floc = "%s/stat_test/agg/matched_hashes/%s/" % (self.base_floc, self.max_sellable_date)

        client.join(sellable_hash,client_clean.hem == sellable_hash.hem, "left") \
            .agg(countDistinct(sellable_hash.hem).alias("matched_hem"), \
                 countDistinct(when(col("region_p") == "US", sellable_hash.hem)).alias("US_matched_hem")) \
            .coalesce(1) \
            .write \
            .csv(mh_floc, header=True, mode="overwrite")
                 

        # Sellable pair breakdown #
        
        sp_floc = "%s/stat_test/agg/sellable_pair_breakdown/%s/" % (self.base_floc, self.max_sellable_date)

        client \
            .join(sellable_hash,client_clean.hem == sellable_hash.hem, "inner") \
            .groupBy(col("cookie_domain_p").alias("cDomain")) \
            .agg(count(col("cookie")).alias("matched_pairs"), \
                 count(when(col("region_p") == "US", col("cookie"))).alias("US_matched_pairs")) \
            .coalesce(1) \
            .write \
            .csv(sp_floc, header=True, mode="overwrite")

    def match_test(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .appName("match_test") \
            .config("hive.metastore.client.factory.class", 
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")

        # Generate lidid temp file #

        lidid_q = """SELECT hash as hem FROM auto_dmps.all_features_mapping_pair 
                     WHERE cookie RLIKE '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$' 
                     AND date_p = '%s' GROUP BY hash"""  % (self.max_agg_date)

        lidid_floc = "%s/temp/lidid_%s/" % (self.base_floc, self.max_agg_date)

        spark.sql(lidid_q).coalesce(10000).write.parquet(lidid_floc, mode="overwrite")

        # Load lidid temp file as temp table #

        spark.read.parquet(lidid_floc).createOrReplaceTempView("lidid")

        # Create maid temp table #

        maid_q = """SELECT piiidentifier as hem FROM auto_sellable.sellable_pair 
                    WHERE cookie_domain_p IN ('aaid','idfa') 
                    AND date_p = '%s' GROUP BY 1""" % (self.max_sellable_date)

        spark.sql(maid_q).createOrReplaceTempView("maid")

        # Create client temp table #

        client_q = """SELECT REPLACE(hem,'"') hem 
                      FROM %s""" % (self.client_table)

        spark.sql(client_q).createOrReplaceTempView("client")

        # Generate boolean file#

        boolean_q = """SELECT a.hem, b.hem IS NOT NULL has_lidid, c.hem IS NOT NULL has_maid 
                     FROM client a 
                     LEFT JOIN lidid b ON a.hem = b.hem 
                     LEFT JOIN maid c ON a.hem = c.hem 
                     GROUP BY 1,2,3"""

        boolean_floc = "s3n://ds-emr-storage/jira/seid/%s/match_test/lidid_w_booleans/" % (ticket)

        spark.sql(boolean_q).write.parquet(boolean_floc)

fullcontact = seid("fullcontact","seid_55")

fullcontact.stat_test()
 