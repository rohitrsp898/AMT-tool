
import config,main_webm
import pandas as pd
#import snowflake.connector
import pypyodbc
from datetime import datetime
import logging

#logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)

def dataframe(db_sch_tb):
    try:
        #DS_SF is dsn 64 bit ODBC driver for Snowflake
        connection = pypyodbc.connect(f'DSN=DS_SF;UID={config.snow_username};PWD={config.snow_password};')

        df1=pd.read_sql(f'''SELECT * from {db_sch_tb}''',connection)
        with open("Output/report.txt",'a',newline='') as f:
            f.write(f"Snowflake MVP table : {db_sch_tb}\n")
            f.write(f"Snowflake MVP '{db_sch_tb}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"Snowflake MVP '{db_sch_tb}' table Columns count : {df1.shape[1]}\n")
            f.write(f"Snowflake MVP '{db_sch_tb}' table records count : {df1.shape[0]}\n\n")

        print("Snowflake MVP table: ",db_sch_tb.split('.')[-1])
        print(f"Snowflake MVP '{db_sch_tb}' table Columns :",tuple(df1.columns))
        print(f"Snowflake MVP '{db_sch_tb}' table Columns count :",df1.shape[1])
        print(f"Snowflake MVP '{db_sch_tb}' table records count :",df1.shape[0],"\n")
        return df1
    except Exception as e:
            print(e)
            with open('Output/errors.txt', 'a',newline='') as f:
                f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
                main_webm.status="ERROR: Snowflake MVP Something Went Wrong !!"


def details(db_sch_tb):
    try:

        # Create connection with oracle database
        con = pypyodbc.connect(f'DSN=DS_SF;UID={config.snow_username};PWD={config.snow_password};')
        
        df1=pd.read_sql(f'''SELECT * from {db_sch_tb} limit 1''',con)
        cursor=con.cursor()
        cursor.execute(f'''SELECT COUNT(*) FROM {db_sch_tb}''')
        records=cursor.fetchall()
        #print(type(records))
        with open("Output/basic_details.txt",'a',newline='') as f:
            f.write(f"Snowflake MVP table : {db_sch_tb}\n")
            f.write(f"Snowflake MVP '{db_sch_tb}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"Snowflake MVP '{db_sch_tb}' table Columns count : {df1.shape[1]}\n")
            f.write(f"Snowflake MVP '{db_sch_tb}' table records count : {records[0][0]}\n\n")

        print("Snowflake MVP table: ",db_sch_tb.split('.')[-1])
        print(f"Snowflake MVP '{db_sch_tb}' table Columns :",tuple(df1.columns))
        print(f"Snowflake MVP '{db_sch_tb}' table Columns count :",df1.shape[1])
        print(f"Snowflake MVP '{db_sch_tb}' table records count :",records[0][0],"\n")
        

    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="[Snowflake MVP] Something Went Wrong !!"

#.IDEA_QA_TEST_DB.A_IDEA_T_STG_FIN_CARD_TEMP_34
#print(dataframe("FINANCE.FINANCE.FIN_DISP"))
        
#details("FINANCE.FINANCE.FIN_DISP")
# Checking connection to Snowflake MVP

#cursor=con.cursor()
# # Execute a statement that will generate a result set.
#sql = f"SELECT * from {db}.{schm}.{tb}"
#cursor.execute(sql)
# # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
#df = pd.read_sql(sql, connection)
#print("\n --------------------------- ORACLE SOURCE -----------------------------\n")
#print("Snowflake MVP table: ",tb)
#print("\n")


