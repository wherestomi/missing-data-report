import pandas as pd
import azure_cnxn as az
from sqlalchemy import create_engine

def start(points_path, save_path, date):
    #Selecting the file paths of the ISP data for each county TOBOLA server & create a data frame
    ap = pd.DataFrame(pd.read_csv(fr"{points_path}"))

    ###DATA CLEANING
    firstname = pd.DataFrame(ap["EE Name"].str.split(",", expand=True, n=1)[0])
    ap["FirstName"] = firstname
    lastname = pd.DataFrame(ap["EE Name"].str.split(",", expand=True, n=1)[1])
    ap["LastName"] = lastname

    a_date = pd.DataFrame(ap["Actual Time"].str.split(" ", expand=True, n=1)[0])
    a_time = pd.DataFrame(ap["Actual Time"].str.split(" ", expand=True, n=1)[1])
    ap["ActualDate"] = a_date
    ap["ActualTime"] = a_time

    s_date = pd.DataFrame(ap["Scheduled Time"].str.split(" ", expand=True, n=1)[0])
    s_time = pd.DataFrame(ap["Scheduled Time"].str.split(" ", expand=True, n=1)[1])
    ap["ScheduledDate"] = s_date
    ap["ScheduledTime"] = s_time

    #un-needed columns
    ap = ap.drop(['EE Name', 'Actual Time', 'Scheduled Time'], axis=1)

    #rename columns & change data types for SQL analysis
    ap.columns = ['ee_code',
                  'date',
                  'exception',
                  "Minutes_Points_Off",
                  'Overridden',
                  'Overriden_By',
                  'Points',
                  'tier',
                  'FirstName',
                  'LastName',
                  'ActualDate',
                  'ActualTime',
                  'ScheduledDate',
                  'ScheduledTime']

    ap["ActualDate"] = pd.to_datetime(ap.ActualDate)
    ap["ScheduledDate"] = pd.to_datetime(ap.ScheduledDate)
    ap["ActualTime"] = a_time = pd.to_datetime(ap.ActualTime).dt.time
    ap["ScheduledTime"] = pd.to_datetime(ap.ScheduledTime).dt.time

    ap.to_csv(fr"{save_path}\ATNpoints({date}).csv")

    return(ap)




def write_to_table(DataFrame):
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replatement from the new data pull

    ap = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    ap.to_sql("atnPoints", engine, index=False, if_exists='replace')



