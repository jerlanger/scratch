from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from datetime import datetime, timedelta


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
        self.AADate = date

    def imp_join(self, lookback=1):

        impDate = self.runDate - timedelta(days=lookback)
        self.impDate = impDate

        impCheck = self.spark.sql("""SELECT *
        FROM auto_userverssp.impression
        WHERE date_p = '%s'
        LIMIT 1""" % (impDate))

        rawImps = self.spark.sql("""SELECT 
        %s daysSince,
        md5,
        request.ip,
        dev_type,
        dev_maker,
        nbrowser,
        os,
        category_id
        FROM auto_userverssp.impression
        WHERE date_p = '%s'""" % (lookback, impDate))

        self.impDateExists = len(impCheck.head(1)) > 0

        #self.imp_date_check(dataset=rawImps)

        self.filterImps = rawImps.join(self.AAEmail, on="md5", how="inner")
        self.filterImps.cache()

    def imp_date_check(self, dataset=None):

        if dataset is not None:
            dataset.withColumn("date_part", f.when(f.col("daysSince") <= 30, 30)
                        .when(f.col("daysSince") <= 45, 45)
                        .when(f.col("daysSince") <= 60, 60)
                        .when(f.col("daysSince") <= 90, 90)
                        .when(f.col("daysSince") <= 180, 180)
                        .otherwise(181))\
                .groupBy("date_part")\
                .agg(f.countDistinct("daysSince").alias("dates"))\
                .write\
                .csv("s3://ds-emr-storage/jira/AAReturn/%s/imp_date_check/" % (self.runDate), mode="overwrite")

    def hash_match_counts(self):

        imps = self.filterImps

        imps.groupBy("md5")\
            .agg(f.sum(f.when(f.col("daysSince") <= 30, 1)).alias("ct_30"),
                 f.sum(f.when(f.col("daysSince") <= 60, 1)).alias("ct_60"),
                 f.sum(f.when(f.col("daysSince") <= 90, 1)).alias("ct_90"),
                 f.sum(f.when(f.col("daysSince") <= 180, 1)).alias("ct_180"))\
            .coalesce(500)\
            .write.parquet("s3://ds-emr-storage/jira/AAReturn/%s/temp/hash_match_counts/%s/" % (self.AADate, self.impDate), mode="ignore")

    def user_agent_counts(self):

        imps = self.filterImps

        imps.filter(f.col("daysSince") <= 45)\
            .groupBy("md5", "ip", "dev_type", "dev_maker", "nbrowser", "os")\
            .agg(f.count("*").alias("ct_45"))\
            .coalesce(1000)\
            .write.parquet("s3://ds-emr-storage/jira/AAReturn/%s/temp/user_agent_counts/%s/" % (self.AADate, self.impDate), mode="ignore")

    def iab_category_counts(self):

        imps = self.filterImps

        imps.filter(f.col("daysSince") <= 45)\
            .groupBy("md5", "category_id")\
            .agg(f.count("*").alias("ct_45"))\
            .coalesce(500)\
            .write.parquet("s3://ds-emr-storage/jira/AAReturn/%s/temp/iab_category_counts/%s/" % (self.AADate, self.impDate), mode="ignore")

    def collate_results(self):

        resultsHM = self.spark.read.parquet("s3://ds-emr-storage/jira/AAReturn/%s/temp/hash_match_counts/" % (self.AADate))
        resultsUA = self.spark.read.parquet("s3://ds-emr-storage/jira/AAReturn/%s/temp/user_agent_counts/" % (self.AADate))
        resultsIC = self.spark.read.parquet("s3://ds-emr-storage/jira/AAReturn/%s/temp/iab_category_counts/" % (self.AADate))

        resultsHM.groupBy("md5")\
                 .agg(f.sum("ct_30").alias("ct_30"),
                      f.sum("ct_60").alias("ct_60"),
                      f.sum("ct_90").alias("ct_90")
                      )\
                 .coalesce(10)\
                 .write\
                 .csv("s3://ds-emr-storage/jira/AAReturn/%s/results/hash_match_counts/" % (self.AADate), header=False, mode="overwrite")

        resultsUA.groupBy("md5", "ip", "dev_type", "dev_maker", "nbrowser", "os")\
                 .agg(f.sum("ct_45").alias("ct_45"))\
                 .coalesce(50)\
                 .write\
                 .csv("s3://ds-emr-storage/jira/AAReturn/%s/results/user_agent_counts/" % (self.AADate), header=False, mode="overwrite")

        resultsIC.groupBy("md5", "category_id")\
                 .agg(f.sum("ct_45").alias("ct_45"))\
                 .coalesce(50)\
                 .write\
                 .csv("s3://ds-emr-storage/jira/AAReturn/%s/results/iab_cateogry_counts/" % (self.AADate), header=False, mode="overwrite")

    def unpersist_imp(self):

        self.filterImps.unpersist()

    def full_run(self):

        n = 1
        skipDates = [datetime.date(datetime.strptime("2020-11-22", "%Y-%m-%d"))]
        # need to skip date 2020-11-12, as it is malformed
        
        while n <= 181:
            self.imp_join(lookback=n)

            if self.impDateExists and self.impDate not in skipDates:
                self.hash_match_counts()
                if n <= 45:
                    self.user_agent_counts()
                    self.iab_category_counts()
                print("date complete %s (%s lookback)" % (self.impDate, n))
            else:
                print("empty day %s (%s lookback)" % (self.impDate, n))

            self.unpersist_imp()
            n += 1

        self.collate_results()


run = StatsAA()
run.full_run()
