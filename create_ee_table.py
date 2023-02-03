
import notion_df as nd
import pandas as pd
nd.pandas()

notion_token = "secret_omL8nzIdOZySUeAtSOCHm0bUNh2ydXdohuePKPBXkxm"
ee_db_id = "03764697bdf74f2b938313815cf62069"
id_url = "https://www.notion.so/03764697bdf74f2b938313815cf62069?v=e856d446c1a44cfcb8857b014f591284"

ee_df = pd.DataFrame(nd.download(ee_db_id, api_key=notion_token, resolve_relation_values=True))

updated = pd.read_csv(fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\Data_Pulls\2023\1_January\1.31.23\RAW\CurrentEmployees.csv")

ee_df = ee_df[["EE Code",
               'First Name',
               'Last Name',
               'Hire Date',
               'Termination Date']]

updated = updated[['Employee_Code',
                  'Legal_Firstname',
                  'Legal_Lastname',
                  'Hire_Date',
                  'Termination_Date']]

ee_df.columns = ['EE_Code', 'First_Name', 'Last_Name', 'Hire_Date', 'Termination_Date']
updated.columns = ['EE_Code', 'First_Name', 'Last_Name', 'Hire_Date', 'Termination_Date']


def start(ee_path, save_path, savedate):

    # Create a dataframe from the excel file
    ee = pd.DataFrame(pd.read_csv(fr"{ee_path}"))

    ee.to_csv(fr"{save_path}\EE({savedate}).csv")

    return(ee)


def write_to_table(DataFrame):
    import azure_cnxn as az
    from sqlalchemy import create_engine
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replatement from the new data pull

    old = ee_df
    new = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    old.to_sql("OLD", engine, index=False, if_exists='replace')

    ##  Create Table
    import sqlalchemy as sql
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    new.to_sql("NEW", engine, index=False, if_exists='replace')

    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = sql.create_engine(cnxn_url)

    ee_update_query = """Select
    c.EE_Code as 'EE Code',
    c.First_Name as 'First Name',
    c.Last_Name as 'Last Name',
    c.Hire_Date as 'Hire Date',
     CASE 
        WHEN
            c.Termination_Date = '00/00/0000'
        THEN NULL
        ELSE c.Termination_Date
    END as 'Termination Date'
FROM
    OLD o 
RIGHT JOIN
    NEW c 
    ON
        o.EE_Code = c.EE_Code
WHERE
    o.EE_Code is NULL and c.First_Name not like 'Test'"""
    updates = pd.read_sql_query(ee_update_query, con=engine)
    print("New Employees to NOTION")
    print(updates)
    updates = updates

    updates.to_notion(id_url, title="Tests", api_key=notion_token)

write_to_table(updated)