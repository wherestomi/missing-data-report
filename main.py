import create_atn_table
import create_isp_table
import create_timecard_table
import isp_table_clean
import missing_data_query
import pandas as pd
from sqlalchemy.engine import URL
import sqlalchemy as sql
import azure_cnxn as az



date = input("What's Today's Date?:   ")
save_path = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\Data_Pulls\10_October\{date}"
nc_isp_path = fr"{save_path}\RAW\nc_isp.xlsx"
kc_isp_path = fr"{save_path}\RAW\kc_isp.xlsx"
q1 = fr"{save_path}\RAW\q1atn.xlsx"
q2 = fr"{save_path}\RAW\q2atn.xlsx"
q3 = fr"{save_path}\RAW\q3atn.xlsx"
q4 = fr"{save_path}\RAW\q4atn.xlsx"
timecard_path = fr"{save_path}\RAW\timecards.csv"


isp_table = create_isp_table.start(kc_isp_path, nc_isp_path, save_path, date)
atn_table = create_atn_table.start(q1, q2, q3, q4, save_path, date)
timecard_table = create_timecard_table.start(timecard_path, save_path, date)

create_isp_table.write_to_table(isp_table)
create_atn_table.write_to_table(atn_table)
create_timecard_table.write_to_table(timecard_table)


# Once saved, remember to go into the TimeCard csv file and null out any OutDays == '0000-00-00'
#   and any OutTimes == '12:00:00 AM'. These are MISSING out dates/times that are being recorded as 0


cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
engine = sql.create_engine(cnxn_url)

clean_data = pd.read_sql(isp_table_clean.query, engine)
az.cursor.execute(clean_data)
az.cursor.commit()
result = pd.read_sql(missing_data_query.query, engine)
az.cursor.execute(result)
az.cursor.commit()
result = pd.DataFrame(result)

result.to_csv(fr"{save_path}\MissingData({savedate}).csv")