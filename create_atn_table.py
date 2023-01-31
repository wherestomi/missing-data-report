import pandas as pd
import azure_cnxn as az
from sqlalchemy import create_engine

def start(atn1, atn2, atn3, save_path, date):
    # enter that paths to the files for each quater of attendance
    path_list = []
    q1_path = atn1
    q2_path = atn2
    q3_path = atn3
    final_path = save_path
    path_list += [q1_path, q2_path, q3_path, final_path]


    # Create a frame list
    frame_list = []

    q1 = pd.DataFrame(pd.read_excel(fr"{q1_path}"))
    q2 = pd.DataFrame(pd.read_excel(fr"{q2_path}"))
    q3 = pd.DataFrame(pd.read_excel(fr"{q3_path}"))
    frame_list += [q1, q2, q3]

    # Create dataframes for the past 6 months
    h1 = q1.append(q2, ignore_index= True)
    ytd = h1.append(q3, ignore_index= True)
    frame_list += [h1, ytd]

    # Update Column Names
    for frame in frame_list:
        frame.columns = ['program_site',
                         'individual',
                         'date',
                         'attendance',
                         'status',
                         'entered_date',
                         'entered_by',
                         'time_zone']

    # Update necessary column datatypes/values
    for frame in frame_list:
        frame.date = pd.to_datetime(frame.date)
        frame['entered_date'] = pd.to_datetime(frame['entered_date'])

    # Data Cleaning (Individual Names)
    ytd['individual'] = ytd['individual'].replace(["James, Janet"], 'James, Janet M')
    ytd['individual'] = ytd['individual'].replace(["Chituck, Christina"], 'Chituck, Christina L')
    ytd['individual'] = ytd['individual'].replace(["Wooters, Brianna"], 'Wooters, Brianna E')
    ## E104
    ytd['individual'] = ytd['individual'].replace(["Wright, Ralph"], 'Wright, Ralph W')
    ytd['individual'] = ytd['individual'].replace(["Seward, Robert"], 'Seward, Robert')
    ## J101
    ytd['individual'] = ytd['individual'].replace(["LeVan, Charles"], 'LeVan, Charles J')
    ## K110
    ytd['individual'] = ytd['individual'].replace(["GREEN, JOSEPH E"], 'GREEN, JOSEPH E E')
    ## 3NL
    ytd['individual'] = ytd['individual'].replace(["Gallagher, James"], 'Gallagher, James M')
    ytd['individual'] = ytd['individual'].replace(["Garrison, Christian"], 'Garrison, Christian')
    ytd['individual'] = ytd['individual'].replace(["Lanier, Daniel"], 'Lanier, Daniel L')
    ## 8NL
    ytd['individual'] = ytd['individual'].replace(["Jardon-Rosales, Dulce"], 'Jardon-Rosales, Dulce Y')
    ytd['individual'] = ytd['individual'].replace(["Goldsberry, Nyea"], 'Goldsberry, Nyea Nicole')
    ## Castlebrook
    ytd['individual'] = ytd['individual'].replace(["Faust, Travis"], 'Faust, Travis A')
    ytd['individual'] = ytd['individual'].replace(["Headen, Deven"], 'Headen, Deven T')

    #### End


    ytd.to_csv(fr"{save_path}\Attendance({date}).csv")

    return(ytd)

def write_to_table(DataFrame):
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replatement from the new data pull

    atn = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    atn.to_sql("Attendance2022", engine, index=False, if_exists='replace')