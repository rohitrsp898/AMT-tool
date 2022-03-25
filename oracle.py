
from datetime import datetime
import cx_Oracle
import config,main_webm
import pandas as pd
import logging

#logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)

# # making dsn string
# dsn_tns = cx_Oracle.makedsn(config.o_hostname, config.o_port, config.o_sid)
# # Create connection with oracle database
# connection = cx_Oracle.connect(config.o_username, config.o_password, dsn_tns)

def dataframe(table):
    #print("\n --------------------------- ORACLE SOURCE -----------------------------\n")
    #print("ORACLE table: ",table)
    #print("\n")
    try:
        # making dsn string
        dsn_tns = cx_Oracle.makedsn(config.o_hostname, config.o_port, config.o_sid)
        # Create connection with oracle database
        connection = cx_Oracle.connect(config.o_username, config.o_password, dsn_tns)

        df1=pd.read_sql(f'''SELECT * FROM {table}''',connection)
        with open("Output/report.txt",'a',newline='') as f:
            f.write(f"ORACLE table : {table}\n")
            f.write(f"ORACLE '{table}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"ORACLE '{table}' table Columns count : {df1.shape[1]}\n")
            f.write(f"ORACLE '{table}' table records count : {df1.shape[0]}\n\n")

        print("ORACLE table: ",table)
        print(f"ORACLE '{table}' table Columns :",tuple(df1.columns))
        print(f"ORACLE '{table}' table Columns count :",df1.shape[1])
        print(f"ORACLE '{table}' table records count :",df1.shape[0],"\n")
        return df1
        
    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f: 
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="ERROR: Oracle Something Went Wrong !!"

def details(table):
    try:
        dsn_tns = cx_Oracle.makedsn(config.o_hostname, config.o_port, config.o_sid)
        # Create connection with oracle database
        con = cx_Oracle.connect(config.o_username, config.o_password, dsn_tns)

        df1=pd.read_sql(f'''SELECT * FROM {table} WHERE ROWNUM = 1;''',con)
        cursor=con.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        records=cursor.fetchall()
        with open("Output/basic_details.txt",'a',newline='') as f:
            f.write(f"ORACLE table : {table}\n")
            f.write(f"ORACLE '{table}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"ORACLE '{table}' table Columns count : {df1.shape[1]}\n")
            f.write(f"ORACLE '{table}' table records count : {records}\n\n")
     

    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f: 
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="[Oracle] Something Went Wrong !!"
# oracle("INVENTORIES")

# # Execute a statement that will generate a result set.
#sql = f"SELECT * from {db}.{schm}.{tb}"
#cursor.execute(sql)
# df=dataframe('INVENTORIES')
# print(df)


