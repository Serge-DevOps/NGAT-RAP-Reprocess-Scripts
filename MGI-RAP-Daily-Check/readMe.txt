All the changes can be done in .env file. Do not change NGAT_RAP_Reprocess v2.py

.env variables: ----------------------------------

auth_token = '<<sentry token>>'
api_key    = '<<RAP API Key>>'
api_resend_pw = '<<RAP API Pw>>'
smtp_server = 'mhsmail.mhs.com'
sender_email = 'noreply@mhs.com'
receiver_email = 'DevelopmentSupport@MHS.com'
optional_csv_file = ''
sentry_api_interval = ''
--------------------------------------------------

If you leave smtp_server empty, like this: smtp_server = ''
It will not try to send the e-mail, so if you want to run locally, set it to ''
 
You can now indicate the csv file path and the sentry API Interval on .ENV, no need to modify the script and save it, just change .ENV if you need to run it locally with a CSV, custom interval, etc, and skip sending e-mails
 
If you leave optional_csv_file  as '', it will just set it to None and use the Sentry API to get the data
If you leave sentry_api_interval as '', it will just use the default 24h, so no need to inform that

To send email to multiple emails use comma in between emails:
receiver_email = 'breno.assis@mhs.com,serge.voloshenko@mhs.com'
cc_email = 'DevelopmentSupport@MHS.com, marcel@mhs.com'