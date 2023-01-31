import pandas as pd
import azure_cnxn as az
from sqlalchemy import create_engine

def start(kc_isp_path, nc_isp_path, save_path, date):
    #Selecting the file paths of the ISP data for each county TOBOLA server & create a data frame
    kc_isp = pd.DataFrame(pd.read_excel(fr"{kc_isp_path}"))
    ncc_isp = pd.DataFrame(pd.read_excel(fr"{nc_isp_path}"))

    all_isp = ncc_isp.append(kc_isp, ignore_index= True)

    ###DATA CLEANING

    #un-needed columns
    all_isp = all_isp.drop(['Site Name', 'Status', 'EVV Supporting Document', 'Duration (hh:mm)'], axis=1)

    #rename columns & change data types for SQL analysis
    all_isp.columns = ['form_id',
                       'site',
                       'individual',
                       "isp_program",
                       'entered_by',
                       'date',
                       'billable',
                       'begin_time',
                       'end_time',
                       'duration',
                       'location',
                       'comments',
                       'group_count',
                       'time_zone']

    all_isp.date = pd.to_datetime(all_isp['date'])
    all_isp.begin_time = pd.to_datetime(all_isp.begin_time).dt.time
    all_isp.end_time = pd.to_datetime(all_isp.end_time).dt.time

    #### Clean Individual Names ####

    ## E103
    all_isp['individual'] = all_isp['individual'].replace(["James, Janet"], 'James, Janet M')
    all_isp['individual'] = all_isp['individual'].replace(["Chituck, Christina"], 'Chituck, Christina L')
    all_isp['individual'] = all_isp['individual'].replace(["Wooters, Brianna"], 'Wooters, Brianna E')
    ## E104
    all_isp['individual'] = all_isp['individual'].replace(["Wright, Ralph"], 'Wright, Ralph W')
    all_isp['individual'] = all_isp['individual'].replace(["Seward, Robert"], 'Seward, Robert')
    ## J101
    all_isp['individual'] = all_isp['individual'].replace(["LeVan, Charles"], 'LeVan, Charles J')
    ## K110
    all_isp['individual'] = all_isp['individual'].replace(["GREEN, JOSEPH E"], 'GREEN, JOSEPH E E')
    ## 3NL
    all_isp['individual'] = all_isp['individual'].replace(["Gallagher, James"], 'Gallagher, James M')
    all_isp['individual'] = all_isp['individual'].replace(["Garrison, Christian"], 'Garrison, Christian')
    all_isp['individual'] = all_isp['individual'].replace(["Lanier, Daniel"], 'Lanier, Daniel L')
    ## 8NL
    all_isp['individual'] = all_isp['individual'].replace(["Jardon-Rosales, Dulce"], 'Jardon-Rosales, Dulce Y')
    all_isp['individual'] = all_isp['individual'].replace(["Goldsberry, Nyea"], 'Goldsberry, Nyea Nicole')
    ## Castlebrook
    all_isp['individual'] = all_isp['individual'].replace(["Faust, Travis"], 'Faust, Travis A')
    all_isp['individual'] = all_isp['individual'].replace(["Headen, Deven"], 'Headen, Deven T')

    #### End






    all_isp.to_csv(fr"{save_path}\TOTAL_ISP({date}).csv")

    return(all_isp)




def write_to_table(DataFrame):
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replatement from the new data pull

    all_isp = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    all_isp.to_sql("ISP", engine, index=False, if_exists='replace')



