import pandas as pd
import azure_cnxn as az
from sqlalchemy import create_engine

def start(pdf_path, save_path, date):
    #Selecting the file paths of the ISP data for each county TOBOLA server & create a data frame
    pdf = pd.DataFrame(pd.read_csv(fr"{pdf_path}"))

    ###DATA CLEANING

    #un-needed columns

    #rename columns & change data types for SQL analysis
    

    pdf.to_csv(fr"{save_path}\PDFs({date}).csv")

    return(pdf)




def write_to_table(DataFrame):
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replatement from the new data pull

    pdf = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    pdf.to_sql("WriteUps", engine, index=False, if_exists='replace')



