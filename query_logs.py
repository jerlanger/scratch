class bigquery(object):
    def __init__(self,file_name):
        import datetime
        import sys
        from datetime import timedelta
        
        self.today = datetime.date.today()
        self.yesterday = datetime.date.today() - timedelta(days=1)
        
        if file_name.endswith(".csv"):
            sys.exit("Don't include file type in file name. Default output is .csv")
        else:
            self.file_name = '%s_%s' % (file_name,self.yesterday)
    
    def query_to_csv(self, query, save_location):
        import pandas as pd
        import numpy as np
        
        project_id = 'elite-contact-671'
        
        df = pd.read_gbq(query,project_id, private_key='/usr/local/bi_web_apps/joe_staging/bq_staging/etl_scripts/.secret/bigquery_token.json', dialect='standard')
        
        if 'event_date' not in df.columns:
            df['event_date'] = self.yesterday

        df.drop_duplicates().to_csv('%s%s.csv' % (save_location, self.file_name), index=False)
        
    def csv_to_s3(self, save_location, bucket, bucket_path):
        import boto3
        s3 = boto3.resource('s3')
        
        s3.meta.client.upload_file('%s%s.csv' % (save_location, self.file_name), bucket, '%s%s.csv' %(bucket_path, self.file_name))
    
    def s3_to_redshift(self,rs_table, bucket, bucket_path, key_account='default'):
        import psycopg2 as pg
        import psycopg2.extras
        import secret
        import sys

        if key_account == 'default':
            access_key = secret.li_dev_access_key
            secret_key = secret.li_dev_secret_key
        elif key_account == 'berlin':
            access_key = secret.berlin_access_key
            secret_key = secret.berlin_secret_key
        elif key_account == 'copenhagen':
            access_key = secret.cph_access_key
            secret_key = secret.cph_secret_key
        else:
            sys.exit("incorrect key_account parameter")

        
        redshift = pg.connect(user=secret.admin_user,password=secret.redshift_admin_password,database=secret.rs_db,
                      host=secret.rs_host,port=secret.rs_port)
        dict_cur = redshift.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        
        try:
            dict_cur.execute("""DELETE FROM %s WHERE event_date = '%s'""" %(rs_table,self.yesterday))
            dict_cur.execute("""COPY %s
            FROM 's3://%s/%s%s.csv'
            credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
            csv
            ignoreheader 1""" % (rs_table, bucket, bucket_path, self.file_name, access_key, secret_key))
            print('%s loaded into %s' %(self.file_name, rs_table))
            redshift.commit()      
        except pg.Error as e:
            print('load to %s failed' % rs_table) 
            print(e) 
        
        redshift.close()
        
class athena(object):
    def __init__(self,file_name):
        import datetime
        import sys
        from datetime import timedelta
        
        self.today = datetime.date.today()
        self.yesterday = datetime.date.today() - timedelta(days=1)
        
        if file_name.endswith(".csv"):
            sys.exit("Don't include file type in file name. Default output is .csv")
        else:
            self.file_name = '%s_%s' % (file_name,self.yesterday)
    
    def query_to_csv(self, query, save_location, key_account='default'):
        import pandas as pd
        import numpy as np
        from pyathenajdbc import connect
        import secret
        
        if key_account == 'default':
            access_key = secret.li_dev_access_key
            secret_key = secret.li_dev_secret_key
            staging_dir = secret.li_dev_dir
        elif key_account == 'berlin':
            access_key = secret.berlin_access_key
            secret_key = secret.berlin_secret_key
            staging_dir = secret.berlin_dir
        elif key_account == 'copenhagen':
            access_key = secret.cph_access_key
            secret_key = secret.cph_secret_key
            staging_dir = secret.cph_dir
        else:
            sys.exit("incorrect key_account parameter")
        
        conn = connect(access_key=access_key,
              secret_key=secret_key,
              s3_staging_dir=staging_dir,
              region_name=secret.athena_region)

        df = pd.read_sql(query, conn)
        
        if 'event_date' not in df.columns:
            df['event_date'] = self.yesterday
        
        df.drop_duplicates().to_csv('%s%s.csv' % (save_location, self.file_name), index=False)
 
    def csv_to_s3(self, save_location, bucket, bucket_path):
        import boto3
        s3 = boto3.resource('s3')
        
        s3.meta.client.upload_file('%s%s.csv' % (save_location, self.file_name), bucket, '%s%s.csv' %(bucket_path, self.file_name))
    
    def s3_to_redshift(self,rs_table, bucket, bucket_path, key_account='default'):
        import psycopg2 as pg
        import psycopg2.extras
        import secret
        import sys

        if key_account == 'default':
            access_key = secret.li_dev_access_key
            secret_key = secret.li_dev_secret_key
        elif key_account == 'berlin':
            access_key = secret.berlin_access_key
            secret_key = secret.berlin_secret_key
        elif key_account == 'copenhagen':
            access_key = secret.cph_access_key
            secret_key = secret.cph_secret_key
        else:
            sys.exit("incorrect key_account parameter")

        
        redshift = pg.connect(user=secret.admin_user,password=secret.redshift_admin_password,database=secret.rs_db,
                      host=secret.rs_host,port=secret.rs_port)
        dict_cur = redshift.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        
        try:
            dict_cur.execute("""DELETE FROM %s WHERE event_date = '%s'""" %(rs_table,self.yesterday))
            dict_cur.execute("""COPY %s
            FROM 's3://%s/%s%s.csv'
            credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
            csv
            ignoreheader 1""" % (rs_table, bucket, bucket_path, self.file_name, access_key, secret_key))
            print('%s loaded into %s' %(self.file_name, rs_table))
            redshift.commit()      
        except pg.Error as e:
            print('load to %s failed' % rs_table) 
            print(e) 
        
        redshift.close()
