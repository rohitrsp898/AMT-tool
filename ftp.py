
import config,main_webm
from datetime import datetime
import pandas as pd
import logging

#logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)


# Create Dataframe from FTP object, Count the number of rows, columns and get columns names
def dataframe(ftp_file_path):
    #print("\n --------------------------- FTP SOURCE -----------------------------\n")
    print("FTP file path: ",ftp_file_path+"\n")
    #print("\n")
    try:
        path=f"ftp://{config.ftp_username}:{config.ftp_password}@{config.ftp_host}:{config.ftp_port}/{ftp_file_path}"
        if path.endswith('.csv'):
            df1=pd.read_csv(path)
        if path.endswith('.txt'):
            df1=pd.read_csv(path,sep='¬',engine='python')

        with open("Output/report.txt",'a',newline='') as f:
            f.write(f"Ftp file Path : {ftp_file_path}\n")
            f.write(f"Ftp file Columns : {tuple(df1.columns)}\n")
            f.write(f"Ftp file Columns count : {df1.shape[1]}\n")
            f.write(f"Ftp file records count : {df1.shape[0]}\n\n")
        

        print("Ftp file Columns :",tuple(df1.columns))
        print("Ftp file Columns count :",df1.shape[1])
        print("Ftp file records count :",df1.shape[0],"\n")
        return df1

    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="ERROR: FTP Something Went Wrong !!"
       # logging.error("Exception: %s",e)


# df=dataframe("/idea_demo/synthetic_data_200_000.csv")
# print(df)
#    /ideadatamigration/fullload/IDEA_TEST_PHASE_STG.ABC_T_STG_FIN_LOAN.txt
#    /idea_demo/synthetic_data_200_000.csv