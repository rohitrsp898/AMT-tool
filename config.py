from configparser import ConfigParser

file='.init\config.ini'
config = ConfigParser()
config.read(file)

#s3
service_name=config["s3"]["service_name"]
region_name=config["s3"]["region_name"]
aws_access_key_id=config["s3"]["aws_access_key_id"]
aws_secret_access_key=config["s3"]["aws_secret_access_key"]

#ftp 
ftp_host=config["ftp"]["hostname"]
ftp_port=config["ftp"]["port"]
ftp_username=config["ftp"]["username"]
ftp_password=config["ftp"]["password"]

#sftp    
sftp_host=config["sftp"]["hostname"]
sftp_port=config["sftp"]["port"]
sftp_username=config["sftp"]["username"]
sftp_password=config["sftp"]["password"]

#oracle
o_hostname=config["oracle"]["hostname"] 
o_port=config["oracle"]["port"]
o_username=config["oracle"]["username"]
o_password=config["oracle"]["password"]
o_sid=config["oracle"]["sid"]

#snowflake  
snow_host=config["snow-mvp"]["hostname"]
snow_username=config["snow-mvp"]["username"]
snow_password=config["snow-mvp"]["password"]

snowqa_host=config["snow-qa"]["hostname"]
snowqa_username=config["snow-qa"]["username"]
snowqa_password=config["snow-qa"]["password"]


#sql server
ss_server=config["sqlserver"]["server"]
ss_username=config["sqlserver"]["username"]
ss_password=config["sqlserver"]["password"]
ss_database=config["sqlserver"]["database"]

#redshift
red_host=config["redshift"]['hostname']
red_username=config['redshift']['username']
red_password=config["redshift"]['password']
red_port=config["redshift"]['port']
red_db=config["redshift"]['db_name']

#teradata
td_host=config["teradata"]["hostname"]
td_port=config["teradata"]["port"]
td_username=config["teradata"]["username"]
td_password=config["teradata"]["password"]