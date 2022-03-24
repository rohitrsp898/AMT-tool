
# import boto3,json
# import config,main_webm
# import pandas as pd
# import io, logging
# from datetime import datetime
# from smart_open import smart_open

# #logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)
# # Get Column names, Columns count, rows count and return Dataframe from S3 object
# s3_client = boto3.client(
#                 service_name=config.service_name,
#                 region_name=config.region_name,
#                 aws_access_key_id=config.aws_access_key_id,
#                 aws_secret_access_key=config.aws_secret_access_key)

# s3_uri="s3://ideakafkatest/QA_UI_testing/ftp/6/oracle/C#DEMO_USER/INVENTORIES/load_date=23-02-2022/load_time=135122/part-00000-f50a5d21-bfa3-4e2b-8338-752e4282dca3-c000.snappy.parquet"

# records:int = 0
# run=True
# columns=[]
# bucket_name=s3_uri.split('/')[2]
# s3_file_path=s3_uri.split(bucket_name)[1][1:]
# #print("--------",s3_file_path)
# #s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{s3_file_path}'
# print("S3 file path: ",f"s3://{bucket_name}/{s3_file_path}")

# theobjects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)
# for obj in theobjects['Contents']:
#     if obj['Key'].endswith('.csv'):
#         #print(obj['Key'])
#         if run:    # Get Columns name and Columns count from S3 object
#             resp = s3_client.select_object_content(
#                 Bucket=bucket_name,
#                 Key=obj['Key'], 
#                 ExpressionType='SQL',
#                 Expression='''SELECT * FROM s3object limit 1''',
#                 #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#                 InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#                 OutputSerialization={'CSV': {}},
#             )
#             #print("Connection stablished with s3...")
#             for event in resp['Payload']:
#                 if 'Records' in event:
#                     cols = event['Records']['Payload'].decode('utf-8')
#                     columns=cols.split(',')
#                     #print("-----------------------------------------------------------------------\n")
#                     print("S3 Sink file Columns : ",tuple(columns))
#                     print("S3 Sink file Columns count: ",len(columns))

#                     run=False
#         # Get Rows count from S3 object
        
#         resp = s3_client.select_object_content(
#             Bucket=bucket_name,
#             Key=obj['Key'], 
#             ExpressionType='SQL',
#             Expression='''SELECT count(*) FROM s3object''',
#             InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#             #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#             OutputSerialization={'CSV': {}},
#         )
#         for event in resp['Payload']:
#             if 'Records' in event:
#                 records += int(event['Records']['Payload'].decode('utf-8'))
#                 #print("-----------------------------------------------------------------------")

#     if obj['Key'].endswith('.json'):
#         #print(obj['Key'])
#         if run:    # Get Columns name and Columns count from S3 object
#             resp = s3_client.select_object_content(
#                 Bucket=bucket_name,
#                 Key=obj['Key'], 
#                 ExpressionType='SQL',
#                 Expression='''SELECT * FROM s3object limit 1''',
#                 #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#                 InputSerialization={'JSON': {"Type": "Document"}},
#                 OutputSerialization={'JSON': {}},
#             )
#             #print("Connection stablished with s3...")
#             for event in resp['Payload']:
#                 if 'Records' in event:
#                     cols = event['Records']['Payload'].decode('utf-8')
#                     print(list(json.loads(cols).keys()))
#                     columns=list(json.loads(cols).keys())
#                     #print("-----------------------------------------------------------------------\n")
#                     print("S3 Sink file Columns : ",tuple(columns))
#                     print("S3 Sink file Columns count: ",len(columns))

#                     run=False
#         # Get Rows count from S3 object
        
#         resp = s3_client.select_object_content(
#             Bucket=bucket_name,
#             Key=obj['Key'], 
#             ExpressionType='SQL',
#             Expression='''SELECT count(*) FROM s3object''',
#             InputSerialization={'JSON': {"Type": "Document"}}, # Comment this line while reading Parquet file
#             #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#             OutputSerialization={'CSV': {}},
#         )
#         for event in resp['Payload']:
#             if 'Records' in event:
#                 records += int(event['Records']['Payload'].decode('utf-8'))
#                 #print("-----------------------------------------------------------------------")

#     if obj['Key'].endswith('.parquet'):
#         #print(obj['Key'])
#         if run:    # Get Columns name and Columns count from S3 object
#             resp = s3_client.select_object_content(
#                 Bucket=bucket_name,
#                 Key=obj['Key'], 
#                 ExpressionType='SQL',
#                 Expression='''SELECT * FROM s3object limit 1''',
#                 #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#                 InputSerialization={'Parquet': {}},
#                 OutputSerialization={'JSON': {}},
#             )
#             #print("Connection stablished with s3...")
#             for event in resp['Payload']:
#                 if 'Records' in event:
#                     cols = event['Records']['Payload'].decode('utf-8')
#                     print(list(json.loads(cols).keys()))
#                     columns=list(json.loads(cols).keys())
#                     #columns=cols.split(',')
#                     #print("-----------------------------------------------------------------------\n")
#                     print("S3 Sink file Columns : ",tuple(columns))
#                     print("S3 Sink file Columns count: ",len(columns))

#                     run=False
#         # Get Rows count from S3 object
        
#         resp = s3_client.select_object_content(
#             Bucket=bucket_name,
#             Key=obj['Key'], 
#             ExpressionType='SQL',
#             Expression='''SELECT count(*) FROM s3object''',
#             InputSerialization={'Parquet': {}}, # Comment this line while reading Parquet file
#             #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#             OutputSerialization={'CSV': {}},
#         )
#         for event in resp['Payload']:
#             if 'Records' in event:
#                 records += int(event['Records']['Payload'].decode('utf-8'))
#                 #print("-----------------------------------------------------------------------")
#     print("S3 Sink file Rows count: ",records)