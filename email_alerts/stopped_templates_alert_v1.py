
import psycopg2 as pg
import psycopg2.extras
import smtplib as smtp
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
sys.path.append('~/github/je_scripts/github_scripts/scratch/local/')
import secret

def email_alert(sql):
    conn = pg.connect(user=secret.admin_user,password=secret.redshift_admin_password,database=secret.rs_db,
                      host=secret.rs_host,port=secret.rs_port)
    dict_cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    
    try:
        dict_cur.execute(sql)
        stalled_list = dict_cur.fetchall()          
    except pg.Error as e:
        print 'failed'
        print e  
        
    conn.close()
    
    sender = 'sender_email@example.com'
    subject = 'Stalled Template Alert!'
    ccaddr = ['cc1@example.com','cc2@example.com']
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['Subject'] = subject
    msg['CC'] = ', '.join(ccaddr)
    
    slicer_start = str(datetime.date.today() - datetime.timedelta(days=14))
    slicer_end = str(datetime.date.today() - datetime.timedelta(days=1))

    
    smtpObj = smtp.SMTP('smtp.mandrillapp.com',587)
    smtpObj.ehlo()
    smtpObj.login(secret.email_login,secret.email_password)
    print 'logged in to email server'
    
    for record in stalled_list:
        
        am_name = record['am_name']
        email = record['email_address'].strip()
        media_group_id = str(record['media_group_id'])
        media_group_name = record['media_group_name']
        pub_lfx_id = record['pub_lfx_id']
        publisher_id = str(record['publisher_id'])
        publisher_name = record['pub_name']
        template_id = str(record['template_id'])
        template_name = record['template_name']
        yesterday_decisions = str(record['decisions'])
        previous_day_of_week_decisions = str(record['pdow_decisions'])
        this_week_decisions = str(record['week_decisions'])
        previous_week_decisions = str(record['previous_week_decisions'])
                
        toaddr = [email]
        
        if yesterday_decisions == 0:
            stalled = 'did not participate'
        else:
            stalled = 'declined significantly'
        
        intro = """ Hi there %s, <br> <br> A template you are the PDM for &mdash; %s (id: %s) &mdash; %s yesterday. Please check it out. <br>""" %(am_name,template_name,template_id,stalled)
        details = """ <br> <u>Details</u> <br> <br> <b>Media Group Name</b>: %s (id: %s) <br> <b>Publisher Name</b>: %s (id: %s) <br> <b>Template Name</b>: %s (id: %s) <br> <b>Decisions Yesterday</b>: %s <br> <b>Decisions Same Day Last Week</b>: %s <br> <b>Total Decisions This Week</b>: %s <br> <b>Total Decisions Last Week</b>: %s <br>""" %(media_group_name,media_group_id,publisher_name,publisher_id,template_name,template_id,yesterday_decisions,previous_day_of_week_decisions,this_week_decisions,previous_week_decisions)
        links = """ <br> <a href="https://lfm.liveintent.com/publisher/details/?pid=%s#tab=templates">LFM Link</a> &#9474; <a href="https://uslicer.iponweb.com/liveintent/UI/Reports/UUID?order_by=granularity_day&order_direction=ASC&chart_column=decisions&granularity=day&normalization=value&parent_match=equals&parent_key=template_id&parent_value=%s&category=granularity_day&start_date=%s&end_date=%s&id_list=total">uSlicer Link</a>""" %(pub_lfx_id,template_id,slicer_start,slicer_end)
        
        body = intro + details + links
        
        if msg.has_key('to'):
            msg.replace_header('to',email)
        else:
            msg['To'] = email
            
        msg.set_payload(MIMEText(body, 'html'))
        content = msg.as_string()
            
        smtpObj.sendmail(sender, toaddr, content)
        print 'email sent to %s' %email

    smtpObj.quit()

sql1 = """SELECT * FROM dsa.email_alert_template_decline_v1"""
    
email_alert(sql1)
