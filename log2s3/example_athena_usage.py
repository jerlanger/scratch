from query_logs import athena

# Variables for ETL #

save_location = "/local/folder/location/example/"
s3_bucket = "dsa-joe"
s3_bucket_path = "s3_folder/example_location"
rs_table = "dsa.example_table"

query = """Athena Query Here"""

# ETL Run #

instance = athena("job_name")

instance.query_to_csv(query,save_location, key_account="default")

instance.csv_to_s3(save_location,s3_bucket,s3_bucket_path)

instance.s3_to_redshift(rs_table,s3_bucket,s3_bucket_path, key_account="default")
