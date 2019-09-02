from query_logs import bigquery

# Variables for ETL #

save_location = "/local/folder/location/example/"
s3_bucket = "dsa-joe"
s3_bucket_path = "s3_subfolder/example_location"
rs_table = "dsa.example_table"

query = """ Standard SQL query here """

# ETL Run #

q_run = bigquery("example_name_for_job")

q_run.query_to_csv(query,save_location)

q_run.csv_to_s3(save_location,s3_bucket,s3_bucket_path)

q_run.s3_to_redshift(rs_table,s3_bucket,s3_bucket_path, key_account="default")
