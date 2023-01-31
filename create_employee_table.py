import pandas as pd
import azure_cnxn as az
from sqlalchemy import create_engine

def start(ee_path, save_path, savedate):

    # Create a dataframe from the excel file
    ee = pd.DataFrame(pd.read_csv(fr"{ee_path}"))

    ee.to_csv(fr"{save_path}\EE({savedate}).csv")

    return(ee)


def write_to_table(DataFrame):
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replacement from the new data pull

    ee = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    ee.to_sql("CurrentDSP", engine, index=False, if_exists='replace')