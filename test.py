import create_apt_table
import create_atn_table
import create_isp_table
import create_timecard_table
import create_atn_points_table
import create_pdf_table
import isp_table_clean
import missing_data_query
import pandas as pd
from pandasql import sqldf
from sklearn import datasets
from sqlalchemy.engine import URL
import sqlalchemy as sql
import azure_cnxn as az


date = input("What's Today's Date?:   ")
save_path = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\Data_Pulls\10_October\{date}"
nc_isp_path = fr"{save_path}\CSVs\nc_isp.csv"
kc_isp_path = fr"{save_path}\CSVs\kc_isp.csv"
q1 = fr"{save_path}\CSVs\q1atn.csv"
q2 = fr"{save_path}\CSVs\q2atn.csv"
q3 = fr"{save_path}\CSVs\q3atn.csv"
q4 = fr"{save_path}\CSVs\q4atn.csv"
timecard_path = fr"{save_path}\CSVs\timecards.csv"
apt_path = fr"{save_path}\CSVs\apts.csv"
points_path = fr"{save_path}\CSVs\atnpoints.csv"
pdf_path = fr"{save_path}\CSVs\pdfs.csv"


isp_table = create_isp_table.start(kc_isp_path, nc_isp_path, save_path, date)
atn_table = create_atn_table.start(q1, q2, q3, q4, save_path, date)
timecard_table = create_timecard_table.start(timecard_path, save_path, date)
apt_table = create_apt_table.start(apt_path, save_path, date)
points_table = create_atn_points_table.start(points_path, save_path, date)
pdf_table = create_pdf_table.start(pdf_path, save_path, date)

create_isp_table.write_to_table(isp_table)
create_atn_table.write_to_table(atn_table)
create_timecard_table.write_to_table(timecard_table)
create_apt_table.write_to_table(apt_table)
create_atn_points_table.write_to_table(points_table)


# Once saved, remember to go into the TimeCard csv file and null out any OutDays == '0000-00-00'
#   and any OutTimes == '12:00:00 AM'. These are MISSING out dates/times that are being recorded as 0


cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
engine = sql.create_engine(cnxn_url)

az.cursor.execute(pd.read_sql(isp_table_clean.query, engine))
az.cursor.commit()
az.cursor.execute(pd.read_sql(missing_data_query.query, engine))
az.cursor.commit()
result = pd.DataFrame(pd.read_sql(missing_data_query.query, engine))

result.to_excel(fr"{save_path}\MissingData({savedate}).xlsx")