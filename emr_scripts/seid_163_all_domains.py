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

spark.sql("""SELECT cookie, COLLECT_SET(partner_p) partner_array
                       FROM auto_aggregation.consolidated_partner_link
                       WHERE date_p = %s
                       GROUP BY 1""" %(maxPartnerLinkDate)).createOrReplaceTempView("agg")

spark.sql("""SELECT r.partner, ARRAY_CONTAINS(partner_array,'2380') contains_centro, ARRAY_CONTAINS(partner_array,'88068') contains_taboola, COUNT(*) lidids FROM agg CROSS JOIN UNNEST(partner_array) AS r (partner) GROUP BY 1,2,3""").coalesce(1).write \
.csv("s3n://ds-emr-storage/jira/seid/seid_163/results/partner_links/",mode="overwrite",header=True)