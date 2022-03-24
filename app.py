
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from datetime import datetime
import main_webm
import os,logging
import logging

#logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)


app=Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():

    # if request is post and detail option is selected below code will execute
    if request.method=='POST' and request.form['btn']=='detail':
        print("detail selected")
        
        source=request.form.get('source')
        sink=request.form.get('sink')
        source_path=request.form.get('source_path').strip()
        sink_path=request.form.get('sink_path').strip()

        print(source,sink,source_path,sink_path)
        #call main funtion and getting data to web page
        # if sources are in bewlo list then only main module called
        if source in ['s3_source','teradata','ftp', 'sftp'] and sink in ['s3_sink','snow','snowqa'] and source_path!='' and sink_path!='':
            print("main function called")
            #calling main function for comparison
            main_webm.main(source,sink,source_path,sink_path)
            #print(main_webm.status)
            output_dir=os.path.join(os.getcwd(),'Output',"")
            # Output directory path
            file_locations=f'File Location : {output_dir}'

            if os.path.exists(os.path.join(output_dir,'report.txt')):
                with open(os.path.join(output_dir,"errors.txt"),'r') as f:
                    error=f.read()

            with open(os.path.join(output_dir,'report.txt'), 'r') as f:
                data=f.read()
                return render_template('index.html',file_locations=file_locations,data=data,status=main_webm.status,error=error)
                    
            
        else:
            validation='Please select and fill all the required fields'
            #return redirect(url_for('index'))
            return render_template('index.html',validation=validation)
    
    # if request is post and basic option is selected below code will execute
    if request.method=='POST' and request.form['btn']=='basic':
        print("basic selected")

        source=request.form.get('source')
        sink=request.form.get('sink')
        source_path=request.form.get('source_path').strip()
        sink_path=request.form.get('sink_path').strip()

        print(source,sink,source_path,sink_path)
        #call main fn=untion and put data to web page
            
        if source in ['s3_source','teradata'] and sink in ['s3_sink','snow','redshift','snowqa'] and source_path!='' and sink_path!='':
            print("main detail function called")
            main_webm.details(source,sink,source_path,sink_path)
            
            with open(os.path.join(os.getcwd(),'Output','basic_details.txt'), 'r') as f:
                data=f.read()
                print(data)
                return render_template('index.html',data=data,status=main_webm.status)
        elif source in ["ftp", "sftp"]:
            validation='FTP/SFTP source is not supported for basic details !'
            return render_template('index.html',validation=validation)
        else:
            validation='Please select and fill all the required fields'
            return render_template('index.html',validation=validation)
                    
    return render_template("index.html")

        
    # #if "basic" in request.form:
 
    # elif request.method=="POST" and "basic" in request.form:# and request.form['radio']=='basic':
    #     source=request.form.get('source')
    #     sink=request.form.get('sink')
    #     source_path=request.form.get('source_path').strip()
    #     sink_path=request.form.get('sink_path').strip()

    #     print(source,sink,source_path,sink_path)
    #     #call main fn=untion and put data to web page
            
    #     if source in ['s3_source','teradata','sql_server', 'oracle'] and sink in ['s3_sink','snow','redshift'] and source_path!='' and sink_path!='':
    #         print("main function called")
    #         main_webm.details(source,sink,source_path,sink_path)
    #         with open('Output/basic_details.txt', 'r') as f:
    #             data=f.read()
    #             print(data)
    #             return render_template('index.html',data=data,status=main_webm.status)
    #     else:
    #         validation='Please select and fill all the required fields'
    #         #return redirect(url_for('index'))
    #         return render_template('index.html',validation=validation)
    # # elif request.args.get("radio")=='basic':
    # #     return render_template('index.html',source_list=source_list[3:],sink_list=sink_list)
    # else:
    #     return render_template('index.html',src1_list=source1_list,src2_list=source2_list,sink_list=sink_list)               
    # #return render_template("index.html")

    
# @app.route('/basic', methods=['GET', 'POST'])
# def details():
#     if request.method=="POST":
#         source=request.form.get('source')
#         sink=request.form.get('sink')
#         source_path=request.form.get('source_path').strip()
#         sink_path=request.form.get('sink_path').strip()

#         print(source,sink,source_path,sink_path)
#         #call main fn=untion and put data to web page
            
#         if source in ['s3_source','teradata','sql_server', 'oracle'] and sink in ['s3_sink','snow','redshift'] and source_path!='' and sink_path!='':
#             print("main function called")
#             main_webm.details(source,sink,source_path,sink_path)
#             with open('Output/basic_details.txt', 'r') as f:
#                 data=f.read()
#                 print(data)
#                 return render_template('details.html',data=data,status=main_webm.status)
#         else:
#             validation='Please select and fill all the required fields'
#             #return redirect(url_for('index'))
#             return render_template('details.html',validation=validation)
                    
#     return render_template("details.html")




if __name__ == '__main__':
    app.run(debug=True)