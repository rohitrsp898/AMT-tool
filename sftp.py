import config,main_webm
import pandas as pd
from smart_open import smart_open
from datetime import datetime
import logging

#logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)

def dataframe(sftp_file_path):
    #print("\n --------------------------- FTP SOURCE -----------------------------\n")
    print("sFTP file path: ",sftp_file_path+"\n")
    #print("\n")
    try:
        path=f"sftp://{config.sftp_username}:{config.sftp_password}@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}"
        #print("sFTP file path: ---->",path)
        df1=pd.read_csv(smart_open(path))
        with open("Output/report.txt",'a',newline='') as f:
            f.write(f"sFtp file Path : {sftp_file_path}\n")
            f.write(f"sFtp file Columns : {tuple(df1.columns)}\n")
            f.write(f"sFtp file Columns count : {df1.shape[1]}\n")
            f.write(f"sFtp file records count : {df1.shape[0]}\n\n")


        print("sFtp file Columns :",tuple(df1.columns))
        print("sFtp file Columns count :",df1.shape[1])
        print("sFtp file records count :",df1.shape[0],"\n")
        return df1

    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="ERROR: SFTP Something Went Wrong !!"
# path="/sftpuser1/demo-data/facts/INVENTORIES.csv"
# p2="/sftpuser1/demo-data/synthetic_data_200_000/synthetic_data_200_000.csv"
# print(dataframe(p2))