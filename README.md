# Automated Migration Testing (AMT) Tool 

This script is basically to compare data from two different systems.
This module is to help the users to check if the processed data matches the source system data.
This tool is written on the python3 version and mostly based on the Pandas Dataframe, where it compares two Pandas Dataframes and generates reports.

Below are the source and sink systems-
-	Source - FTP, sFTP, Teradata. Oracle and SQL Server.
-	Sink - AWS S3, Redshift and Snowflake.

Functionality-
1.	It will compare source and sink dataframes if there is any mismatched it will store those details into separate csv and also generate simple reports using datacompy package.
2.	It will generate five csv files and two text files as below-


•	Matched_records.csv - All the matched records of Source and Sink will be stored here.

•	Missmatched_records.csv – All the miss-matched records of Source and Sink will be stored here.

•	Missing_records.csv – All the missing records from Sink will be stored here.

•	Duplicate_records.csv – All the duplicate records between Source and Sink will be stored here.

•	Compared_records.csv – All the shuffled records will be stored here.

•	reports.txt – Summary report of source and sink comparison.

•	errors.txt – All the exception and errors will be stored here.


Requirements-
Tools - Python 3.8 (64 bit) or higher, Excel or any text editor tool to view the csv file.

Additional odbc application - Oracle instant client light 64 bit, Snowflake 64 bit, SQL SERVER odbc driver 64 bit.

Packages -
flask,
cx_Orcle,
pyarrow,
pypyodbc,
pandas,
boto3,
smart_open,
paramiko,
datacompy,
redshift_connector,
teradatasql

