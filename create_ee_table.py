import notion_df as nd
import pandas as pd
import csv2notion
nd.pandas()

notion_token = "secret_omL8nzIdOZySUeAtSOCHm0bUNh2ydXdohuePKPBXkxm"
# Test employee Database information used to work on changes before deployment
# id_url = 'https://www.notion.so/7678202864044c9bb9f23e2972452902?v=ef9d3a8f60844dbaaf17f252162775fd'
# ee_db_id = "7678202864044c9bb9f23e2972452902"

ee_db_id = "03764697bdf74f2b938313815cf62069"
id_url = "https://www.notion.so/03764697bdf74f2b938313815cf62069?v=e856d446c1a44cfcb8857b014f591284"

ee_df = pd.DataFrame(nd.download(ee_db_id, api_key=notion_token, resolve_relation_values=True))

updated = pd.read_csv(fr"/Users/tomiawodiya/Desktop/20230207131024_Current Employee Notion_67f1afd5.csv")
save_path = fr"/Users/tomiawodiya/Desktop"


ee_df = ee_df[["EE Code",
               'First Name',
               'Last Name',
               'Hire Date',
               'Shift',
               'Rotation',
               'Position Seat',
               'Direct Supervisor',
               'Home Base',
               'DL Expiration Date',
               'Termination Date',
               'Status']]
updated = updated[['Employee_Code',
                   'Legal_Firstname',
                   'Legal_Lastname',
                   'Hire_Date',
                   'Position_Seat_Number',
                   'Primary_Supervisor',
                   'Department',
                   'Termination_Date']]
updated.columns = ['EE Code',
                   'First Name',
                   'Last Name',
                   'Hire Date',
                   'Position Seat',
                   'Supervisor',
                   'Department',
                   'Termination Date']


def start(ee_path, save_path, savedate):

    # Create a dataframe from the excel file
    ee = pd.DataFrame(pd.read_csv(fr"{ee_path}"))

    ee.to_csv(fr"{save_path}\EE({savedate}).csv")

    print("""Paycom Current
          """)

    print(ee.columns)
    return(ee)

def write_to_table(DataFrame, savepath):
    import azure_cnxn as az
    import sqlalchemy as sql
    from sqlalchemy import create_engine
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replacement from the new data pull

    # Limit columns to those that are to be the same between Notion & Paycom and/or to be updated
    old = ee_df[['EE Code',
                 'First Name',
                 'Last Name',
                 'Hire Date',
                 'Position Seat',
                 'Termination Date',
                 'Status']]
    new = DataFrame[['Employee_Code',
                   'Legal_Firstname',
                   'Legal_Lastname',
                   'Hire_Date',
                   'Position_Seat_Number',
                   'Termination_Date']]
    new.columns = ['EE Code',
                   'First Name',
                   'Last Name',
                   'Hire Date',
                   'Position Seat',
                   'Termination Date']

    ##  Create SQL Server Connection
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)

    # Write tables to sql server database
    old.to_sql("notion_ee", engine, index=False, if_exists='replace')
    new.to_sql("paycom_ee", engine, index=False, if_exists='replace')

    # SQL Query to discover employees who are in Paycom (c), but not in Notion (o)
    ee_update_query = """
        Select
            c.[EE Code] as 'EE Code',
            c.[First Name] as 'First Name',
            c.[Last Name] as 'Last Name',
            c.[Hire Date] as 'Hire Date',
            c.[Position Seat] as 'Position Seat',
         CASE 
            WHEN
                c.[Termination Date] = '00/00/0000'
            THEN NULL
            ELSE c.[Termination Date]
        END as 'Termination Date'
        FROM
            notion_ee o 
        RIGHT JOIN
            paycom_ee c 
            ON
                o.[EE Code] = c.[EE Code]
        WHERE
            o.[EE Code] is NULL and c.[First Name] not like 'Test'"""

    # Create a dataframe from the results of the SQL query displaying "new" employees
    ee_updates = pd.read_sql_query(ee_update_query, con=engine)

    # Dsiplay the results of the query/program
    print("New Employees to NOTION")
    print(ee_updates)

    # Add new employees to the defined Notion database
    ee_updates.to_notion(id_url, title="Tests", api_key=notion_token)

    # List any discrepancies between employee information in new vs old
    ## Current Columns Checked: EE Code, First Name, Last Name, Hire Date, Position Seat, Termination Date.

    discrepancy_query ="""
    SELECT
        CASE
            WHEN p.[EE Code]=n.[EE Code]
                THEN n.[EE Code]
                ELSE 'Error'
            END as 'EE Code',
        p.[First Name],
        p.[Last Name],
        CASE
            WHEN p.[Hire Date] = n.[Hire Date]
                THEN 'Synced'
                ELSE p.[Hire Date]
            END as 'Hire Date',
        CASE
            WHEN p.[Position Seat] = n.[Position Seat]
                THEN 'Synced'
                ELSE p.[Position Seat]
            END as 'Position Seat',
        CASE
            WHEN (((
                CASE
                    WHEN p.[Termination Date] = '00/00/0000'
                    THEN '01/01/2000'
                    ELSE cast(p.[Termination Date] as date)
                END) = (CASE
                        WHEN n.[Termination Date] is NULL
                        THEN '01/01/2000'
                        ELSE n.[Termination Date]
                        END)))
            THEN 'Synced'
            ELSE p.[Termination Date]
        END as 'Termination Date',
    CASE
        WHEN p.[Termination Date] = '00/00/0000'
            THEN 'Active'
        ELSE 'Terminated'
    END as 'Status',
    CASE
        WHEN
            p.[EE Code]=n.[EE Code]
            AND p.[Hire Date] = n.[Hire Date]
            AND ((p.[Position Seat] = n.[Position Seat]) 
                    OR p.[Position Seat] is NULL)
            AND ((
                CASE
                    WHEN p.[Termination Date] = '00/00/0000'
                    THEN '01/01/2000'
                    ELSE cast(p.[Termination Date] as date)
                END) = (CASE
                        WHEN n.[Termination Date] is NULL
                        THEN '01/01/2000'
                        ELSE n.[Termination Date]
                        END))
        THEN 'Confirmed'
        ELSE 'Error'
    END as 'Synced'
FROM 
    paycom_ee p 
JOIN
    notion_ee n 
        ON n.[EE Code]=p.[EE Code]
 """

    discrepancies = pd.read_sql_query(discrepancy_query, con=engine)
    discrepancies = discrepancies.set_index(['EE Code'])
    discrepancies.to_excel(fr"{savepath}/ee_discrepancies.xlsx")
    print(discrepancies)




updated = fr"/Users/tomiawodiya/Desktop/20230207131024_Current Employee Notion_67f1afd5.csv"
table = start(updated,save_path, savedate='2.6.23')
write_to_table(table, save_path)

