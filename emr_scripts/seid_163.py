from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
        .appName("seid_163") \
        .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()
    
spark.catalog.setCurrentDatabase("default")

maxPartnerLinkDate = spark.sql("""SELECT MAX(date_p) FROM auto_aggregation.consolidated_partner_link WHERE partner_p = '2380'""") \
                                .collect()[0][0]
maxFeaturesDate = spark.sql("""SELECT MAX(date_p) FROM auto_dmps.all_features_mapping_pair""") \
                                 .collect()[0][0]

maxMaidDate = spark.sql("""SELECT MAX(date_p) FROM auto_sellable.sellable_pair""") \
                                 .collect()[0][0]

dspCookies = spark.sql("SELECT partner_p, cookie as lidid, partnercookiestring as partnerCookie FROM auto_aggregation.consolidated_partner_link WHERE partner_p IN ('2380','88068') AND date_p = '%s'" %(maxPartnerLinkDate))

dspLIIDs = spark.sql("""SELECT hash as hem, cookie FROM auto_dmps.all_features_mapping_pair 
                     WHERE date_p = '%s'""" %(maxFeaturesDate))

maids = spark.sql("""SELECT cookie_domain_p, piiidentifier as hash, cookie as maid FROM auto_sellable.sellable_pair 
                    WHERE cookie_domain_p IN ('aaid','idfa') 
                    AND date_p = '%s'""" %(maxMaidDate))
                     
baseDSP = dspCookies.join(dspLIIDs, dspCookies.lidid == dspLIIDs.cookie, "inner")

baseWithMaid = baseDSP.join(maids,baseDSP.hem == maids.hash, "inner")

baseWithMaid.groupBy(baseDSP.partner_p,baseWithMaid.cookie_domain_p).agg(F.countDistinct(baseWithMaid.maid).alias("ctD_maids"), \
                                           F.countDistinct(baseWithMaid.hash).alias("ctD_matched_liid"), \
                                           F.count(baseWithMaid.maid).alias("ct_pairs")) \
                                            .coalesce(1).write \
                                            .csv("s3n://ds-emr-storage/jira/seid/seid_163/results/maids/", mode="overwrite")