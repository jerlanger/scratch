from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from datetime import datetime


class StatsAA:

    def __init__(self, date=None):

        self.spark = SparkSession.builder \
            .appName("AA-Validation") \
            .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport() \
            .getOrCreate()

        self.spark.catalog.setCurrentDatabase("auto_userverssp")

        self.runDate = datetime.date(datetime.now())

        if date is None:
            date = self.spark.sql("""SELECT MAX(date_p) FROM auto_external.audience_acuity_email""").collect()[0][0]

        self.AAEmail = self.spark.sql("SELECT md5 FROM auto_external.audience_acuity_email WHERE date_p = '%s' GROUP BY 1" % date)

    def imp_join(self):

        rawImps = self.spark.sql("""SELECT 
        DATEDIFF(CURRENT_DATE - INTERVAL '1' DAY, to_date(date_p,'yyyy-MM-dd')) daysSince ,
        md5,
        request.ip,
        dev_type,
        dev_maker,
        nbrowser,
        os,
        category_id
        FROM auto_userverssp.impression
        WHERE to_date(date_p,'yyyy-MM-dd') <= CURRENT_DATE - INTERVAL '1' DAY
        AND to_date(date_p,'yyyy-MM-dd') > CURRENT_DATE - INTERVAL '181' DAY""")\
            .withColumn("date_part", f.when(f.col("daysSince") <= 30, 30)
                        .when(f.col("daysSince") <= 45, 45)
                        .when(f.col("daysSince") <= 60, 60)
                        .when(f.col("daysSince") <= 90, 90)
                        .when(f.col("daysSince") <= 180, 180)
                        .otherwise(181))

        #self.imp_date_check(dataset=rawImps)
        self.filterImps = rawImps.join(self.AAEmail, on="md5", how="inner").cache()

    def imp_date_check(self, dataset=None):

        if dataset is not None:
            dataset.groupBy("date_part")\
                .agg(f.countDistinct("daysSince").alias("dates"))\
                .write\
                .csv("s3://ds-emr-storage/jira/AAReturn/%s/imp_date_check/" % (self.runDate), mode="overwrite")

    def hash_match_counts(self):

        imps = self.filterImps

        imps.groupBy("md5")\
            .agg(f.sum(f.when(f.col("date_part") == 30, 1)).alias("ct_30"),
                 f.sum(f.when(f.col("date_part") <= 60, 1)).alias("ct_60"),
                 f.sum(f.when(f.col("date_part") <= 90, 1)).alias("ct_90"),
                 f.sum(f.when(f.col("date_part") <= 180, 1)).alias("ct_180"))\
            .coalesce(1000)\
            .write.csv("s3://ds-emr-storage/jira/AAReturn/%s/hash_match_counts/" % (self.runDate), mode="overwrite")

    def user_agent_counts(self):

        imps = self.filterImps

        imps.filter(f.col("date_part") <= 45)\
            .groupBy("md5", "ip", "dev_type", "dev_maker", "nbrowser", "os")\
            .agg(f.count("*").alias("ct_45"))\
            .coalesce(1000)\
            .write.csv("s3://ds-emr-storage/jira/AAReturn/%s/user_agent_counts/" % (self.runDate), mode="overwrite")

    def iab_category_counts(self):

        imps = self.filterImps

        imps.filter(f.col("date_part") <= 45)\
            .groupBy("md5", "category_id")\
            .agg(f.count("*").alias("ct_45"))\
            .coalesce(1000)\
            .write.csv("s3://ds-emr-storage/jira/AAReturn/%s/iab_category_counts/" % (self.runDate), mode="overwrite")

    def full_run(self):

        self.imp_join()
        self.hash_match_counts()
        self.user_agent_counts()
        self.iab_category_counts()


run = StatsAA()
run.full_run()
