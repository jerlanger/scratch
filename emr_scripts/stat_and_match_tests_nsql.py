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
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import countDistinct, col, when, isnull, count

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

        client.join(sellable_hash,client.hem == sellable_hash.hem, "left") \
            .agg(countDistinct(sellable_hash.hem).alias("matched_hem"), \
                 countDistinct(when(col("region_p") == "US", sellable_hash.hem)).alias("US_matched_hem")) \
            .coalesce(1) \
            .write \
            .csv(mh_floc, header=True, mode="overwrite")
                 

        # Sellable pair breakdown #
        
        sp_floc = "%s/stat_test/agg/sellable_pair_breakdown/%s/" % (self.base_floc, self.max_sellable_date)

        client \
            .join(sellable_hash,client.hem == sellable_hash.hem, "inner") \
            .groupBy(col("cookie_domain_p").alias("cDomain")) \
            .agg(count(col("cookie")).alias("matched_pairs"), \
                 count(when(col("region_p") == "US", col("cookie"))).alias("US_matched_pairs")) \
            .coalesce(1) \
            .write \
            .csv(sp_floc, header=True, mode="overwrite")

    def match_test(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import distinct, isnull

        spark = SparkSession.builder \
            .appName("match_test") \
            .config("hive.metastore.client.factory.class", 
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.catalog.setCurrentDatabase("default")

        # Create client temp table #
        
        client = spark.read.csv(self.client_table) \
            .withColumnRenamed("_c0","hem")

        # Generate lidid temp file #

        lidid_q = """SELECT hash as hem FROM auto_dmps.all_features_mapping_pair 
                     WHERE cookie RLIKE '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$' 
                     AND date_p = '%s' GROUP BY hash"""  % (self.max_agg_date)
        
        lidid = spark.sql(lidid_q)

        # Create maid temp table #

        maid_q = """SELECT piiidentifier as hem FROM auto_sellable.sellable_pair 
                    WHERE cookie_domain_p IN ('aaid','idfa') 
                    AND date_p = '%s' GROUP BY 1""" % (self.max_sellable_date)
        
        maid = spark.sql(maid_q)

        # Generate boolean file#

        match_floc = "%s/match_test/standard_match/lidid_%s_maid_%s/" % (self.base_floc, self.max_agg_date, self.max_sellable_date)

        spark.sql(boolean_q).write.parquet(boolean_floc)
        
        client \
            .join(lidid,client.hem == lidid.hem, "left") \
            .join(maid,client.hem == maid.hem, "left") \
            .select(client.hem, lidid.hem.isNotNull().alias("has_lidid"), maid.hem.isNotNull().alias("has_maid")) \
            .distinct() \
            .write \
            .parquet(match_floc)                     

fullcontact = seid("fullcontact","seid_55")

fullcontact.stat_test()
fullcontact.match_test()
 