from query_logs import athena

save_location = "/local/file/location/example/"
s3_bucket = "dsa-joe"
s3_bucket_path = "s3_folder/location/example"
rs_table = "dsa.example_table"

query = """Athena Query Here"""

instance = athena("job_name")

instance.query_to_csv(query,save_location, key_account="default")

instance.csv_to_s3(save_location,s3_bucket,s3_bucket_path)

instance.s3_to_redshift(rs_table,s3_bucket,s3_bucket_path, key_account="default")
