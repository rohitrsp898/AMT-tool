import pypyodbc  #pip install pypyodbc
import config,main_webm
import pandas as pd
import logging

#logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)

###conn = pypyodbc.connect("DRIVER={SQL Server};"+f"SERVER={config.ss_server};UID={config.ss_username};PWD={config.ss_password};DATABASE={config.ss_database}")
# def dataframe(db,table):
#     #print("\n --------------------------- SQL Server SOURCE -----------------------------\n")
#     print("SQL Server table: ",table)
#     #print("\n")
#     try:
          #conn = pypyodbc.connect("DRIVER={SQL Server};"+f"SERVER={config.ss_server};UID={config.ss_username};PWD={config.ss_password};DATABASE={config.ss_database}")
#         df1=pd.read_sql(f'''SELECT * FROM {db}.{table}''',conn)
        #   with open("Output/report.txt",'a',newline='') as f:
        #     f.write(f"ORACLE table : {table}\n")
        #     f.write(f"ORACLE '{table}' table Columns : {tuple(df1.columns)}\n")
        #     f.write(f"ORACLE '{table}' table Columns count : {df1.shape[1]}\n")
        #     f.write(f"ORACLE '{table}' table records count : {df1.shape[0]}\n\n")

#         print("SQL Server table: ",table)
#         print(f"SQL Server '{table}' table Columns :",tuple(df1.columns))
#         print(f"SQL Server '{table}' table Columns count :",df1.shape[1])
#         print(f"SQL Server '{table}' table records count :",df1.shape[0],"\n")
#         return df1
#     except Exception as e:
#         print(e)
#         with open('Output/errors.txt', 'a',newline='') as f:
#             f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
#             main_webm.status="ERROR: SQL Server Something Went Wrong !!"

# df=dataframe_sql("dbo.tbl_test")
# df=pd.read_sql("SELECT * from dbo.table",conn)
# print(df)

#print()

