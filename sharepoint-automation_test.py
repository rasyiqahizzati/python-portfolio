from argparse import ArgumentParser
from logger import AppLogger
import connectionconf
import pandas as pd
from sqlalchemy import create_engine, sql
import urllib.parse
from timeit import default_timer as timer
from datetime import datetime, timedelta
from dateutil import parser, relativedelta
import os
import re
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.http.request_options import RequestOptions
from office365.sharepoint.files.file import File
from sharepoint_utils import sharepoint_buildurl
import send_email
import calendar
import re


def process(logger, input_param, conn_detail, process_date=None, schedule_date=None):
    logger.debug("[%s] Started for daily" % (input_param['tablename']))
    totalStartTime = timer()

    # create postgresql engine connection string
    postgres_engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (
        conn_detail['username'],
        urllib.parse.quote_plus(conn_detail['password']),
        conn_detail['host'],
        conn_detail['port'],
        conn_detail['database']
    ))
    
    # default getting the current date from system datetime during execution
    # relativedelta reset the hour, minute and second to 00:00:00
    curDate = datetime.now() + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0) 

    # if schedule date is not none, then take schedule date as more accurate
    # it is base on the scheduled wake up time
    if schedule_date != None:
        logger.debug("schedule date [%s]" % (schedule_date))
        curDate = schedule_date
    elif process_date != None:
        # process date is pass in manual
        logger.debug("Process Date [%s]" % (process_date))
        curDate = process_date

    # connect to the database
    conn = postgres_engine.connect()

    # Calculate and format last day of the current month (follows format of "month" column in data)
    curDate = datetime.now() + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)

    ctx = ClientContext('https://um018.sharepoint.com/sites/ISDBIReportingTeam').with_credentials(UserCredential('bda.reports@u.com.my', 'Nvt4Zp+n'))

    # sharepoint paths on where to get the new files, where to move the files once it's been proccessed/failed
    sharepoint_path = '/sites/ISDBIReportingTeam/Shared Documents/Share folder/Customer Experience Dashboard/NPS Dashboard'
    sharepoint_path_processed = '/sites/ISDBIReportingTeam/Shared Documents/Share folder/Customer Experience Dashboard/NPS Dashboard/Processed'
    sharepoint_path_failed = '/sites/ISDBIReportingTeam/Shared Documents/Share folder/Customer Experience Dashboard/NPS Dashboard/Failed'
    local_path = '/user_data01/mapr/pipeline/uat/output/'
    local_filename = 'NPS Test.xlsx'
    local_file = local_path+local_filename
    
    # reusable function to move file between sharepoint directories
    def moveFile(url, name, dir):
        file_info = ctx.web.get_file_by_server_relative_path(url).get().execute_query()
        file_id = file_info.unique_id
        get_file_query_string = "getFileById('{}')".format(file_id)

        moveto_query_params = {"newurl": dir + "/" + name, "flags": 1}
        moveto_query_string = sharepoint_buildurl("moveto", moveto_query_params)

        moveto_url = "/".join(
            [ctx.service_root_url(), "web", get_file_query_string, moveto_query_string]
        )
        request = RequestOptions(moveto_url)
        request.method = "POST"
        ctx.pending_request().execute_request_direct(request)
        
    root_folder = ctx.web.get_folder_by_server_relative_path(sharepoint_path)
    # todo this function lists all files in the directory including subdirectories, need to take performance impact into account in future and explore alternate functions.
    files = root_folder.get_files(True).execute_query()

    # a pair of matching file name and the final db table name 
    categorySeq = [['Master_Send_TD', 'nps_master_send_td'], ['Master_Response_TD', 'nps_master_response_td'],
                    ['Master_Send_BU', 'nps_master_send_bu'], ['Master_Response_BU', 'nps_master_response_bu']]
    
    for eachCat in categorySeq:

        logger.debug("processing for [%s]" % (eachCat[1]))
    
        # grab ALL the files with the matching name
        matching_files = [
            f
            for f in files
            if eachCat[0] in f.properties["Name"] #search for the files with the matching name
            and f.serverRelativeUrl == sharepoint_path + "/" + f.name
        ]

        # process each matching file
        for f in matching_files:
            # download to the local folder
            file_url = f.properties["ServerRelativeUrl"]
            download_file = os.path.join(
                local_path, os.path.basename(file_url)
                )
            
            # get the year and quarter from the file name 
            file_name = f.properties["Name"]
            pattern = r'_(\d{4})(Q[1-4])'
            match = re.search(pattern, file_name)
            
            if match:
                year = match.group(1)
                quarter = match.group(2)
            
            print(f"Year: {year}, Quarter: {quarter}")
            print(f"Processing for {file_name}")
            
            # open file
            with open(download_file, "wb") as local_file:
                file = (
                    ctx.web.get_file_by_server_relative_path(file_url)
                    .download(local_file)
                    .execute_query()
                )

            # read the file and create a dataframe
            nps_df = pd.read_excel(download_file)

            # ingest to db, temp table first
            nps_df.to_sql('nps_temp', postgres_engine, schema='pxl_temp', if_exists ='replace', index=False) 

            # move the data from temp table to the main table
            # if statements to avoid data type error for each data ingestion
            if eachCat[0] == 'Master_Send_TD':

                updateTableQuery  = """
                    BEGIN;

                    insert into external.{table}
                    select 
                        "MSISDN_Survey ID"::text,
                        "Survey ID"::text,
                        "MSISDN"::text,
                        "PLAN"::text,
                        "FIRST_ACTIVATION_DATE"::timestamp,
                        "RATEPLAN"::text,
                        "CUSTOMER_GROUP"::text,
                        "CURR_STATUS"::text,
                        "TOTAL_TOPUP"::text,
                        "CITY"::text,
                        "STATE"::text,
                        "TENURE_IN_MONTH"::int8,
                        "SUM_INV_AMT"::float8,
                        "TOTAL_COLLECTED"::float8,
                        "OUTSTANDING_BAL"::float8,
                        "TOTAL_OVERDUE"::float8,
                        {year}::int4 as year, 
                        '{quarter}'::text as quarter,
                        '{curDate}'::date as processed_date
                    from pxl_temp.nps_temp;

                    COMMIT;

                    """.format(table = eachCat[1], year = year, quarter = quarter, curDate = curDate.strftime("%Y-%m-%d"))
                conn.execute(sql.text(updateTableQuery)) 

                # move the processed file to another folder
                moveFile(file_url, f.name, sharepoint_path_processed)

            elif eachCat[0] == 'Master_Response_TD':

                updateTableQuery  = """
                    BEGIN;

                    insert into external.{table}
                    select 
                        "MSISDN_Survey ID"::text,
                        "Customer MSISDN"::text,
                        "Customer Name"::text,
                        "SurveyID"::text,
                        "Response Date & Time"::timestamp,
                        "Date"::text,
                        "Time"::text,
                        "Hour"::time,
                        "Question"::text,
                        "Response"::text,
                        "Question Sequence"::text,
                        "Rating"::int8,
                        "PLAN"::text,
                        "FIRST_ACTIVATION_DATE"::timestamp,
                        "RATEPLAN"::text,
                        "CUSTOMER_GROUP"::text,
                        "CURR_STATUS"::text,
                        "TOTAL_TOPUP"::float8,
                        "CITY"::text,
                        "STATE"::text,
                        "TENURE_IN_MONTH"::int8,
                        "SUM_INV_AMT"::float8,
                        "TOTAL_COLLECTED"::float8,
                        "OUTSTANDING_BAL"::float8,
                        "TOTAL_OVERDUE"::float8,
                        {year}::int4 as year, 
                        '{quarter}'::text as quarter,
                        '{curDate}'::date as processed_date
                    from pxl_temp.nps_temp;

                    COMMIT;

                    """.format(table = eachCat[1], year = year, quarter = quarter, curDate = curDate.strftime("%Y-%m-%d"))
                conn.execute(sql.text(updateTableQuery)) 

                # move the processed file to another folder
                moveFile(file_url, f.name, sharepoint_path_processed)

            elif eachCat[0] == 'Master_Send_BU':
                
                updateTableQuery  = """
                    BEGIN;

                    insert into external.{table}
                    select 
                        "MSISDN_Survey ID"::text,
                        "Survey ID"::text,
                        "MSISDN"::text,
                        "Customer"::text,
                        "Creation Time"::timestamp,
                        "Creator Org"::text,
                        "SR ID"::int8,
                        "Creator"::text,
                        "SR Type"::text,
                        "Acceptance Channel"::text,
                        "Customer Group"::text,
                        "Remarks"::text,
                        "Customer ID"::float8,
                        "Alternate contact number"::text,
                        "Has TT"::text,
                        "QC mark"::float8,
                        "CC - Complaint"::text,
                        "Non-FCR"::text,
                        "CSAT"::text,
                        "NPS"::float8,
                        "Rate Plan"::text,
                        "Subscription Status"::text,
                        "Account type"::text,
                        "Account No."::text,
                        "Amount"::float8,
                        "Recipient_Type"::float8,
                        "Order_Operation_Type"::float8,
                        {year}::int4 as year, 
                        '{quarter}'::text as quarter,
                        '{curDate}'::date as processed_date
                    from pxl_temp.nps_temp;

                    COMMIT;

                    """.format(table = eachCat[1], year = year, quarter = quarter, curDate = curDate.strftime("%Y-%m-%d"))
                conn.execute(sql.text(updateTableQuery)) 

                # move the processed file to another folder
                moveFile(file_url, f.name, sharepoint_path_processed)

            elif eachCat[0] == 'Master_Response_BU':
                
                updateTableQuery  = """
                    BEGIN;

                    insert into external.{table}
                    select 
                        "MSISDN_Survey ID"::text,
                        "Customer MSISDN"::text,
                        "Customer Name"::text,
                        "SurveyID"::text,
                        "Response Date & Time"::timestamp,
                        "Date"::text,
                        "Time"::text,
                        "Hour"::time,
                        "Question"::text,
                        "Response"::text,
                        "Question Sequence"::text,
                        "Rating"::int8,
                        "Full Name"::text,
                        "Creation Time"::text,
                        "Creator Org"::text,
                        "SR ID"::int8,
                        "Creator"::text,
                        "SR Type"::text,
                        "Acceptance Channel"::text,
                        {year}::int4 as year, 
                        '{quarter}'::text as quarter,
                        '{curDate}'::date as processed_date
                    from pxl_temp.nps_temp;

                    COMMIT;

                    """.format(table = eachCat[1], year = year, quarter = quarter, curDate = curDate.strftime("%Y-%m-%d"))
                conn.execute(sql.text(updateTableQuery)) 

                # move the processed file to another folder
                moveFile(file_url, f.name, sharepoint_path_processed)

            else: # if the file does not match any of the name pattern
                
                moveFile(file_url, f.name, sharepoint_path_failed)

            # delete df and local file
            del nps_df
            os.remove(download_file)
        
    totalEndTime = timer()
    logger.debug("[%s] Completed monthly data [%0.4fs]" % (input_param['tablename'], totalEndTime - totalStartTime))

if __name__ == "__main__":
    # getting input parameter from CLI
    # default for no argument is under testing
    # for production, it will be trigger with --test='N' or -t 'N'
    inputParser = ArgumentParser()
    inputParser.add_argument("-t", '--test', nargs='?', default='Y', type=str)
    inputParser.add_argument("-d", '--date', nargs='?', default='', type=str)
    args = inputParser.parse_args()

    logger = AppLogger()

    if args.test == 'N':
        # NOTE: Below is for production environment only
        # NOTE: input_param now reside in each of the script file for easy reload/restart
        input_param = {
            "schema": "external",
            "tablename": "nps_master"}

        # TODO: No hard coding on the connection detail, utilise the connectionconf reading using sqlite3
        try:
            if args.date != '':
                process(logger, input_param=input_param, conn_detail=connectionconf.get_db_connection_config("pixel"), schedule_date=parser.parse(args.date))
            else:
                process(logger, input_param=input_param, conn_detail=connectionconf.get_db_connection_config("pixel"))
        except MemoryError:
            logger.error("Out of Memory")
            # raise the error back so that processor can catch this error and log into etl_task_monitoring
            raise ValueError("Out of memory")
        except Exception as e:
            logger.error("Exception: [%s]" % (str(e).strip()))
            # raise the error back so that processor can catch this error and log into etl_task_monitoring
            raise ValueError(str(e).strip())
    else:
        # NOTE: Please put your test code below here
        # once it is confirm, move it to the production section
        # below here you can run it for back dated data
        # by default testing environment will be trigger if you manually
        # execute the script from command line
        # TODO: Parameterize the modele input
        # TODO: No hard coding on the connection detail, utilise the connectionconf reading using sqlite3
        print ("testing")
        # trigger for current day
        input_param = {
            "schema": "external",
            "tablename": "nps_master"}
        process(logger, input_param=input_param, conn_detail=connectionconf.get_db_connection_config("pixel"))

        # trigger for multiple day range
        # startDate = parser.parse('2023-06-01 00:00:00')
        # endDate = parser.parse('2023-06-30 00:00:00')

        # curDate = startDate
        # while curDate <= endDate:
        #     process(logger, input_param, connectionconf.get_db_connection_config("pixel"), curDate)
        #     curDate += timedelta(days=1)
