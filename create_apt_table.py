import pandas as pd

def start(apt_table, save_path, date):
    # Create DataFrame from Excel File
    
    apt = pd.DataFrame(pd.read_excel(fr"{apt_table}"))
    
    # Update Column names
    apt.columns = ['Form_ID',
                   'program',
                   'Site',
                   'individual',
                   'Entered_By',
                   'Reported_By',
                   'date',
                   'begin_time',
                   'End_Time',
                   'provider',
                   'specialty',
                   'reason',
                   'description',
                   'Location_Type',
                   'follow_up_date',
                   'Address',
                   'Location',
                   'Phone',
                   'Driver',
                   'Pick Up At',
                   'Depart Time',
                   'Status',
                   'Notification Level',
                   'apt_status',
                   'comment',
                   'time_zone']
    
    # Drop unnecessary columns
    apt.drop(columns = ['Site',
              'Entered_By',
              'Reported_By',
              'End_Time',
              'Location_Type',
              'Address',
              'Location',
              'Phone',
              'Driver',
              'Pick Up At',
              'Depart Time',
              'Status',
              'Notification Level'], axis=1)

    apt["date"] = pd.to_datetime(apt.date)
    apt["begin_time"] = pd.to_datetime(apt['begin_time']).dt.time
    
    
    apt.to_csv(fr"{save_path}\Appointments({date}).csv")
    
    return(apt)


def write_to_table(DataFrame):
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replatement from the new data pull
    import azure_cnxn as az
    import sqlalchemy as sql

    apt = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = sql.create_engine(cnxn_url)
    apt.to_sql("Appointments2022", engine, index=False, if_exists='replace')
