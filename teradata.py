import teradatasql
from datetime import datetime
import pandas as pd
import config, main_webm

def dataframe(db_tb):
    try:
        with teradatasql.connect(host=config.td_host, user=config.td_username, password=config.td_password) as connect:
            df1 = pd.read_sql(f'select * from {db_tb};', connect)
            with open("Output/report.txt",'a',newline='') as f:
                f.write(f"Teradata table : {db_tb}\n")
                f.write(f"Teradata '{db_tb}' table Columns : {tuple(df1.columns)}\n")
                f.write(f"Teradata '{db_tb}' table Columns count : {df1.shape[1]}\n")
                f.write(f"Teradata '{db_tb}' table records count : {df1.shape[0]}\n\n")
                
            #print(data)
    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f: 
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="ERROR: [Teradata] Something Went Wrong !!"


def details(db_tb):
    try:
        with teradatasql.connect(host=config.td_host, user=config.td_username, password=config.td_password) as connect:
            df1 = pd.read_sql(f'select top 1 * from {db_tb};', connect)
            cursor = connect.cursor()
            rec=cursor.execute(f'select count(*) from {db_tb};')
            records=rec.fetchall()
            #print(records[0][0])
            with open("Output/basic_details.txt",'a',newline='') as f:
                f.write(f"Teradata table : {db_tb}\n")
                f.write(f"Teradata '{db_tb}' table Columns : {tuple(df1.columns)}\n")
                f.write(f"Teradata '{db_tb}' table Columns count : {df1.shape[1]}\n")
                f.write(f"Teradata '{db_tb}' table records count : {records[0][0]}\n\n")
    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f: 
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="[Teradata] Something Went Wrong !!"

#print(details("IDEA_QA_TEST_DB.IDEA_Perf_Load_Test15"))