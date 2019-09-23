def pcg(app,startDate,endDate=None): 
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from datetime import date, timedelta, datetime
    
    spark = SparkSession.builder \
        .appName("pcgCookieGeneration") \
        .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.catalog.setCurrentDatabase("default")
    
    graphMaxDate = spark.sql("""SELECT MAX(date_p) FROM auto_dmps.all_features_mapping_pair""").collect()[0][0]
    
    if endDate == None:
        endDate = graphMaxDate
        
    start = datetime.strptime(startDate, '%Y%m%d')
    end = datetime.strptime(endDate, '%Y%m%d')
    
    baseS3Loc = "s3n://li-identity-evidence-data-production/"
    s3Paths = []    

    delta = end - start
    for i in range(delta.days + 1):
        day = (start + timedelta(days=i)).strftime('%Y/%m/%d/*/')
        s3Paths.append(baseS3Loc+day)
    
    snowplowReduce = spark.read.format("csv").option("header","false") \
                .option("delimiter","\t") \
                .load(s3Paths) \
                .filter(col("_c0") == app) \
                .select("_c0","_c6","_c8") \
                .withColumnRenamed("_c0","app_id") \
                .withColumnRenamed("_c6","domain_user_id") \
                .withColumnRenamed("_c8","lidid") \
                .cache()
    
    lididCookies = snowplowReduce.filter(col("lidid") <> "null").select("lidid")
    fpcCookies = snowplowReduce.filter(col("domain_user_id") <> "null").select("domain_user_id")
    
    pcgCookies = lididCookies.union(fpcCookies).withColumnRenamed("lidid","cookie").distinct()

    hemQuery = """SELECT hash as hem, cookie
            FROM auto_dmps.all_features_mapping_pair 
            WHERE date_p = '%s' GROUP BY 1,2""" % (graphMaxDate)

    allHem = spark.sql(hemQuery)
    
    pcgHem = allHem.join(pcgCookies,allHem.cookie == pcgCookies.cookie) \
                        .select(allHem.hem) \
                        .distinct()
    
    pcgHemWrite = "s3n://ds-emr-storage/pcg_files/%s/%s_to_%s/" % (app, startDate, endDate)
    
    pcgHem.write.csv(pcgHemWrite, sep="\t", mode="overwrite")
    
    snowplowReduce.unpersist()