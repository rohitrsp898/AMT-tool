
import multiprocessing
import ftp,sftp,snow,snowqa,s3_source,s3_sink,teradata
from datetime import datetime   
from multiprocessing.pool import ThreadPool
import os, numpy as np
import pandas as pd
import datacompy
import logging

#logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)


#start 4 worker processes
pool = ThreadPool(processes=multiprocessing.cpu_count())
#print(multiprocessing.cpu_count())


status=''
def get_dfs(source,source_path,sink,sink_path):
    if source in ['s3_source','teradata','ftp', 'sftp'] and sink in ['s3_sink', 'snow', 'snowqa']:
         async_result1 = pool.apply_async(eval(source).dataframe,(source_path,)) 
         async_result2 = pool.apply_async(eval(sink).dataframe,(sink_path,))          
         df_source=async_result1.get() 
         df_sink=async_result2.get()
         #logging.info("Dataframes created")
         return df_source,df_sink
    else:
        print('source error')
        #logging.error("Dataframes not created")
    


def main(source,sink,source_path,sink_path):
    with open('Output/report.txt', 'w') as f:
        f.write("--------- Summary Report ----------\n")
    with open('Output/errors.txt', 'w') as f:
        f.write("-------- Error Logs --------\n")

    with open('Output/Duplicate_records_sink.csv', 'w') as f:
        f.write("")
    with open('Output/Matched_records.csv', 'w') as f:
        f.write("")
    with open('Output/Mismatched_records.csv', 'w') as f:
        f.write("")
    with open('Output/Compared_records.csv', 'w') as f:
        f.write("")
    with open('Output/Missing_records.csv', 'w') as f:
        f.write("")

    start = datetime.now().replace(microsecond=0)  # Start time
    print("starts at --",start,"\n")
    # Select the Source and Sink 
    df_source,df_sink=get_dfs(source,source_path,sink,sink_path)
    
    # End of Threads and Now Source and Sink dataframes are ready to compare
    # It will compare and generate reports 
    reports(df_source,df_sink)
    # It will compare and store data into CSV file
    process(df_source,df_sink)
    
    end=datetime.now().replace(microsecond=0)      # End time
    print("\nends at - - ",end)
    print("Total Time took - - :",end-start)                # Total time took
    with open('Output/report.txt', 'a',newline='') as f:
        f.write(f"\nTotal Time took - - : {end-start}")
    #os.system("pause")
    


def reports(df_source,df_sink):
    try:
        compare = datacompy.Compare(
        df_source,
        df_sink,
        join_columns=list(df_source.columns),  #You can also specify a list of columns
        abs_tol=0, #Optional, defaults to 0
        rel_tol=0, #Optional, defaults to 0
        df1_name='Source', #Optional, defaults to 'df1'
        df2_name='Sink' #Optional, defaults to 'df2'
        )
        with open('Output/report.txt', 'a') as f:
            f.write(compare.report())

    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f: # Write unmatched records to csv file
            f.write(f"\n--- Exception {datetime.now().replace(microsecond=0)} ---\n{e}\n")

            
def compare(source_df,sink_df):
    try:
        comp=source_df.compare(sink_df,align_axis=0).rename(index={'self': 'Source', 'other': 'Sink'}, level=-1) # Compare the dataframes
        print(comp)
        if comp.shape[0]>0:             # If there are any differences in dataframes
            with open('Output/Compared_records.csv', 'w', newline='') as f:     # Write the differences to csv file
                comp.to_csv(f)
                f.write("\n")
    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f: # Write unmatched records to csv file
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}\n")

# Get unmatched records from Source and Sink & compare both dataframes
def process(source_df,sink_df):
    global status
    #print(source_df,sink_df)
    #compare - If Rows and columns are same but values are different then write the different data to csv
    #print("--------------------------- compare -----------------------------------")
    compare(source_df,sink_df)

    try:
        if sink_df.equals(source_df):            # Check if both dataframes are equal
            print("\nDataframe is Equal")
            # global status
            status="STATUS: Dataframe is equal"
        elif source_df.columns.equals(sink_df.columns):

            #source_df.sort_values(by=list(source_df.columns)[:2],inplace=True)
            #sink_df.sort_values(by=list(sink_df.columns)[:2],inplace=True)
            print("---- Source Dataframe ----\n",source_df)
            print("---- Sink Dataframe ----\n",sink_df)
            # source_df['xid']=source_df.index
            # sink_df['xid']=sink_df.index
            # source_df=source_df.reset_index(drop=True)    # Reset index of ftp dataframe
            # sink_df=sink_df.reset_index(drop=True)      # Reset index of s3 dataframe

            source_df['count'] = source_df.groupby(list(source_df.columns)).cumcount()
            sink_df['count'] = sink_df.groupby(list(sink_df.columns)).cumcount()

            source_df['index']=source_df.index
            source_df = source_df.reset_index(drop=True)
            sink_df['index']=sink_df.index
            sink_df = sink_df.reset_index(drop=True)

            # Get the unmatched records from Source and Sink using outer join method
            # Note: It will compare based on count with all the fileds on dataframes to avoid duplicates
            un_df=source_df.merge(sink_df,how='outer',on=list(source_df.columns[:-1]),indicator=True)
            # print(un_df)
            # Rename the column value of _merge to Source or Sink insted of left_only or right_only
            un_df["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)
            # Rename the column index_x to Source_index and index_y to Sink_index
            un_df.rename(columns = {'index_x':'Source_index','index_y':"Sink_index"}, inplace = True)
            un_df["Source_index"].replace({np.nan: "Missing"}, inplace=True)
            un_df["Sink_index"].replace({np.nan: "Missing"}, inplace=True)
           
            
            if un_df[lambda x: x['_merge']=='both'].shape[0]>0:
                #print("\nDataframe is Not Equal\n")
                with open('Output/Matched_records.csv', 'w',newline='') as f: # Write unmatched records to csv file
                    un_df[lambda x: x['_merge']=='both'].drop(['count'], axis=1).to_csv(f)
                    f.write("\n")

            if un_df[(un_df["_merge"] == 'Sink') & (un_df["count"] != 0)].shape[0]>0:
                #print("\nDataframe is Not Equal\n")
                with open('Output/Duplicate_records_sink.csv', 'w',newline='') as f: # Write unmatched records to csv file
                    un_df[(un_df["_merge"] == 'Sink') & (un_df["count"] != 0)].groupby(un_df.columns.tolist()[:-4]).size().reset_index().rename(columns={0:'records_count'}).to_csv(f)
                    f.write("\n")

            if un_df[(un_df["_merge"] == 'Source') & (un_df["count"] == 0)].shape[0]>0:
                #print("\nDataframe is Not Equal\n")
                with open('Output/Missing_records.csv', 'w',newline='') as f: # Write unmatched records to csv file
                    un_df[(un_df["_merge"] == 'Source') & (un_df["count"] == 0)].drop(['count'], axis=1).to_csv(f)
                    f.write("\n")

            if un_df[lambda x: x['_merge']!='both'].shape[0]>0:
                #print("\nDataframe is Not Equal\n")
                with open('Output/Mismatched_records.csv', 'w',newline='') as f: # Write unmatched records to csv file
                    un_df[lambda x: x['_merge']!='both'].drop(['count'], axis=1).to_csv(f)
                    f.write("\n")
                with open('Output/errors.txt', 'a',newline='') as f: # Write unmatched records to csv file
                    f.write(f"\n--- Message {datetime.now().replace(microsecond=0)} ---\nDataframe have suffled/mismatched records")
                print("\nDataframe have suffled/mismatched records")
                status="STATUS: Dataframe have suffled/mismatched records"
            # else:
            #     with open('Output/errors.txt', 'a',newline='') as f: # Write unmatched records to csv file
            #         f.write(f"\n--- Message {datetime.now().replace(microsecond=0)} ---\nDataframe have suffled/mismatched records")
            #     print("\nDataframe have suffled/mismatched records")
            #     status="STATUS: Dataframe have suffled/mismatched records"
        else:
            with open('Output/errors.txt', 'a',newline='') as f: # Write unmatched records to csv file
                f.write(f"\n--- Message  {datetime.now().replace(microsecond=0)} ---\nSource and Sink Dataframes have different columns.")
            print("\nDataframes have different columns.....")
            status="STATUS: Source and Sink Dataframes have different columns. "
    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a',newline='') as f: # Write unmatched records to csv file
            f.write(f"\n--- Exception {datetime.now().replace(microsecond=0)} ---\n{e}")
        status="ERROR: Something Went Wrong! Plaese provide correct details!!"


def details(source,sink,source_path,sink_path):
    global status
    try:
        with open('Output/basic_details.txt', 'w') as f:
            f.write("Basic Comaprison\n")
            f.write("----------------\n")

        pool.apply_async(eval(source).details,(source_path,)).get()
        
        pool.apply_async(eval(sink).details,(sink_path,)).get()  

        print("\nBasic Details function done !\n")
    
    except Exception as e: 
        print(e)
        status=f"ERROR:{e}"




        
            

