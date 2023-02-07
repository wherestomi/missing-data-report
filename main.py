import create_apt_table
import create_atn_table
import create_ee_table
import create_isp_table
import create_timecard_table
import create_atn_points_table
import create_pdf_table
import isp_table_clean
import pandas as pd
from pandasql import sqldf
from sklearn import datasets
from sqlalchemy.engine import URL
import sqlalchemy as sql
import azure_cnxn as az


date = input("What's Today's Date?:   ")
cut = slice(2)

year = {
    "22": "2022",
    "23": "2023"
}
month = {
    "1.": "1_January",
    "2.": "2_February",
    "3.": "3_March",
    "4.": "4_April",
    "5.": "5_May",
    "6.": "6_June",
    "7.": "7_July",
    "8.": "8_August",
    "9.": "9_September",
    "10": "10_October",
    "11": "11_November",
    "12": "12_December"
}
month_folder = month.get(f'{date[cut]}')
year_folder = year.get(f"{date[-2:]}")

save_path = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\Data_Pulls\{year_folder}\{month_folder}\{date}"
nc_isp_path = fr"{save_path}\RAW\nc_isp.xlsx"
kc_isp_path = fr"{save_path}\RAW\kc_isp.xlsx"
atn1 = fr"{save_path}\RAW\atn1.xlsx"
atn2 = fr"{save_path}\RAW\atn2.xlsx"
atn3 = fr"{save_path}\RAW\atn3.xlsx"
timecard_path = fr"{save_path}\RAW\timecards.csv"
apt_path = fr"{save_path}\RAW\apts.xlsx"
points_path = fr"{save_path}\RAW\atnpoints.csv"
pdf_path = fr"{save_path}\RAW\pdfs.csv"
ee_path = fr"{save_path}\RAW\CurrentEmployees.csv"



isp_table = create_isp_table.start(kc_isp_path, nc_isp_path, save_path, date)
atn_table = create_atn_table.start(atn1, atn2, atn3, save_path, date)
timecard_table = create_timecard_table.start(timecard_path, save_path, date)
apt_table = create_apt_table.start(apt_path, save_path, date)
points_table = create_atn_points_table.start(points_path, save_path, date)
pdf_table = create_pdf_table.start(pdf_path, save_path, date)
ee_table = create_ee_table.start(ee_path, save_path, date)


create_isp_table.write_to_table(isp_table)
create_atn_table.write_to_table(atn_table)
create_timecard_table.write_to_table(timecard_table)
create_apt_table.write_to_table(apt_table)
create_atn_points_table.write_to_table(points_table)
create_pdf_table.write_to_table(pdf_table)
create_ee_table.write_to_table(ee_table, save_path)

cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
engine = sql.create_engine(cnxn_url)


from openpyxl.worksheet.table import Table
from openpyxl.utils import get_column_letter

# Missing Data Query
mdq = """
--DEVEN HEADEN --
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    'Castlebrook' as 'Home',
    'Teena' as 'Manager', 
    'New Castle County' as 'County'

From 
    Attendance2022 atn

    LEFT JOIN isp
        ON atn.date=isp.date 
        AND isp.Individual=atn.individual
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='13B Castlebrook'

    WHERE 
        atn.individual like 'HEAD%'
        AND isp.isp_program is NULL
    

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    tc.InPunchTime>='14:00'
    AND tc.InPunchTime<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  

)

UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
    'Castlebrook' as 'Home',
    'Teena' as 'Manager', 
    'New Castle County' as 'County'

FROM 
    [Attendance2022] atn

        Left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))
        
        Left Join   TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='13B Castlebrook'

WHERE 
    atn.individual like 'HEAD%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
      

    )

    UNION
    (Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    'Castlebrook' as 'Home',
    'Teena' as 'Manager', 
    'New Castle County' as 'County'

FROM 
    [Attendance2022] atn

        Left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12:00 AM' AND isp.[duration]>=30))
        
        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='13B Castlebrook'

WHERE 
    atn.individual like 'HEAD%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3      

)

-- TRAVIS FAUST --
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    'Castlebrook' as 'Home',
    'Teena' as 'Manager', 
    'New Castle County' as 'County'

From 
    Attendance2022 atn

    LEFT JOIN isp
        ON atn.date=isp.date 
        AND isp.Individual=atn.individual
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
                AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='13B Castlebrook'

    WHERE 
        atn.individual like 'FAUST%'
        AND isp.isp_program is NULL
    

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    tc.InPunchTime>='14:00'
    AND tc.InPunchTime<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
      

)

UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
    'Castlebrook' as 'Home',
    'Teena' as 'Manager', 
    'New Castle County' as 'County'

FROM 
    [Attendance2022] atn

        Left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))
        
        Left Join   TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='13B Castlebrook'

WHERE 
    atn.individual like 'FAUST%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  

    )

    UNION
    (Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    'Castlebrook' as 'Home',
    'Teena' as 'Manager', 
    'New Castle County' as 'County'

FROM 
    [Attendance2022] atn

        Left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12:00 AM' AND isp.[duration]>=30))
        
        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='13B Castlebrook'

WHERE 
    atn.individual like 'FAUST%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
      


)

-- CHRISTIAN GARRISON --
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM 
    [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'GARR%'
    AND isp.isp_program is NULL
    -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
    AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='07:00'
    AND cast(tc.InPunchTime as time)<='10:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  






)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'


FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'GARR%'
    AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  






)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
    '3 Nairn' as 'Home',
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
        AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'GARR%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
        AND tc.Department='SA3'

WHERE atn.individual like 'GARR%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  






)

-- DANIEL LANIER -- 

UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM 
    [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'LANI%'
    AND isp.isp_program is NULL
    -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
    AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='07:00'
    AND cast(tc.InPunchTime as time)<='10:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'


FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'LANI%'
    AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
    '3 Nairn' as 'Home',
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
        AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'LANI%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)

UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
        AND tc.Department='SA3'

WHERE atn.individual like 'LANI%'
AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  






)

-- JAMES GALLAGHER --
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM 
    [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'GALL%'
    AND isp.isp_program is NULL
    -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
    AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='07:00'
    AND cast(tc.InPunchTime as time)<='10:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'


FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'GALL%'
    AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
    '3 Nairn' as 'Home',
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
        AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
        AND tc.Department='SA3'

WHERE 
    atn.individual like 'GALL%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '3 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
        AND tc.Department='SA3'

WHERE atn.individual like 'GALL%'
AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)

-- DULCE JARDON-ROSALES -- 
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '8 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM 
    [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA8'

WHERE 
    atn.individual like 'JARD%'
    AND isp.isp_program is NULL
    -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
    AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='07:00'
    AND cast(tc.InPunchTime as time)<='10:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '8 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'


FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA8'

WHERE 
    atn.individual like 'JARD%'
    AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
    '8 Nairn' as 'Home',
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
        AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
        AND tc.Department='SA8'

WHERE 
    atn.individual like 'JARD%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  





)
UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '8 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
        AND tc.Department='SA8'

WHERE atn.individual like 'JARD%'
AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3
  






)

-- ROBERT SEWARD --
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
                'E104' AS 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

      
            left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND tc.Department='W104'

    WHERE atn.individual like 'SEWARD%'
            AND isp.isp_program is NULL
             -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
           -- AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    datepart(hour, tc.InPunchTime)>=7
    AND datepart(hour, tc.InPunchTime)<=10
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




         

)
UNION
(SELECT 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
                'E104' AS 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

             left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
    AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
    ON atn.date=tc.InPunchDay
            AND tc.Department='W104'

    WHERE atn.individual like 'SEWARD%'
            AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




        

)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
                'E104' AS 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

    left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))
        
    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
            AND tc.Department='W104'

    WHERE atn.individual like 'SEWARD%'
           AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3



        


)
UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End', 
                'E104' AS 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'
    FROM [attendance2022] atn

          left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
    ON (atn.date=tc.OutPunchDay)
            AND tc.Department='W104'

    WHERE atn.individual like 'SEWARD%'
           AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay


HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




        

)

-- CHARLES LEVAN -- 
UNION
(SELECT
	atn.individual as 'Name',
	atn.Date as 'Date',
	concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
	datename(weekday, atn.date) as 'Weekday', 
	tc.InPunchTime as 'Shift Start', 
	tc.OutPunchTime as 'Shift End',
                'J101' as 'Home',
                'Whitney' as 'Manager',
                'Kent County' as 'County'


	From TOBOLA..[isp] isp
                Right Join TOBOLA..[attendance2022] atn
                    On atn.date=isp.[date] AND atn.individual=isp.individual
                Right Join TimeCards2022 tc
                    On (concat(datename(weekday, tc.InPunchDay), ', ',datename(MONTH, tc.InPunchDay),' ', datename(day, tc.InPunchDay),', ', datename(year, tc.InPunchDay)))
	=
	concat(datename(weekday, atn.date), ', ',datename(MONTH, atn.date),' ', datename(day, atn.date),', ', datename(year, atn.date))

    Where (atn.Program_Site like '324%' or atn.Program_Site like '104%' or atn.Program_Site like '%101%' or atn.Program_Site like '%110%' or atn.Program_Site like 'west%')
                  AND atn.attendance like '%12%'
    AND tc.EarnHours > 3 AND isp.date is null
                AND atn.individual like 'levan%'
                AND tc.Department='J101'AND (tc.EarnCode='R' OR tc.EarnCode is null)
	
	
Group By atn.date, tc.EarnHours,
 
	atn.individual, 
	isp.[date], 
	atn.date, 
	atn.attendance, 
	tc.Firstname, 
	tc.Lastname, 
	tc.InPunchTime, 
	tc.OutPunchTime, 
	TC.InPunchDay

		
)

-- JOSEPH GREEN -- 
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    'K110' as 'Home',
    'Whitney' as 'Manager',
    'Kent County' as 'County'

FROM [Attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='k110'

WHERE 
    atn.individual like 'gree%'
    AND isp.isp_program is NULL
    -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
    -- AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    datepart(hour, tc.InPunchTime)>=7
    AND datepart(hour, tc.InPunchTime)<=10
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    'K110' as 'Home',
    'Whitney' as 'Manager',
    'Kent County' as 'County'


FROM [Attendance2022] atn

    left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
    AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
    ON atn.date=tc.InPunchDay
    AND tc.Department='k110'

WHERE 
    atn.individual like 'gree%'
    AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3



)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
    'K110' as 'Home',
    'Whitney' as 'Manager',
    'Kent County' as 'County'

FROM [Attendance2022] atn

    left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
    AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
    ON (atn.date=tc.InPunchDay)
    AND tc.Department='k110'

WHERE 
    atn.individual like 'gree%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3



)
UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',                                                                                                                                                                                                                                                           
    'K110' as 'Home',
    'Whitney' as 'Manager',
    'Kent County' as 'County'

FROM [Attendance2022] atn

    left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
    ON (atn.date=tc.OutPunchDay)
    AND tc.Department='k110'

WHERE 
    atn.individual like 'gree%'
    AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay


HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3





)

-- JANET JAMES -- 
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
                'E103' as 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'
    FROM [Attendance2022] atn

            left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND tc.Department='W103'

    WHERE atn.individual like 'JAMES%'
            AND isp.isp_program is NULL
             -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
            AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 
Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    datepart(hour, tc.InPunchTime)>=7
    AND datepart(hour, tc.InPunchTime)<=10
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




         
        

)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
                'E103' as 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'


    FROM [Attendance2022] atn

        left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
    AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
    ON atn.date=tc.InPunchDay
            AND tc.Department='W103'

    WHERE atn.individual like 'JAMES%'
           AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
                'E103' as 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

         left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))
        
    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
            AND tc.Department='W103'

    WHERE atn.individual like 'JAMES%'
            AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3



        

)
UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',  
                'E103' as 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

           left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
    ON (atn.date=tc.OutPunchDay)
            AND tc.Department='W103'

    WHERE atn.individual like 'JAMES%'
           AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay


HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3







)

-- CHRISTINA CHITUCK --

UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
                'E103' as 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'
    FROM [Attendance2022] atn

            left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND tc.Department='W103'

    WHERE atn.individual like 'CHIT%'
            AND isp.isp_program is NULL
             -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
            AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 
Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    datepart(hour, tc.InPunchTime)>=7
    AND datepart(hour, tc.InPunchTime)<=10
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




         
        

)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
                'E103' as 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'


    FROM [Attendance2022] atn

        left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
    AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
    ON atn.date=tc.InPunchDay
            AND tc.Department='W103'

    WHERE atn.individual like 'CHIT%'
           AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3





)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
                'E103' as 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

         left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))
        
    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
            AND tc.Department='W103'

    WHERE atn.individual like 'CHIT%'
            AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3



        

)
UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',  
                'E103' as 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

           left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
    ON (atn.date=tc.OutPunchDay)
            AND tc.Department='W103'

    WHERE atn.individual like 'CHIT%'
           AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay


HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3







)

-- BRIANNA WOOTERS --
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    'E103' as 'Home',
    'Teena' as 'Manager',
    'Kent County' as 'County'

FROM [attendance2022] atn

    left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
    AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


    Left Join TimeCards2022 tc 
    ON atn.date=tc.InPunchDay
    AND tc.Department='W103'

WHERE 
    atn.individual like 'WOOT%'
    AND isp.isp_program is NULL
    -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
    -- AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    datepart(hour, tc.InPunchTime)>=7
    AND datepart(hour, tc.InPunchTime)<=10
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




)

-- RALPH WRIGHT -- 

UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
                'E104' AS 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

      
            left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND tc.Department='W104'

    WHERE atn.individual like 'WRIGHT%'
            AND isp.isp_program is NULL
             -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
           and (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    datepart(hour, tc.InPunchTime)>=7
    AND datepart(hour, tc.InPunchTime)<=10
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




         

)
UNION
(SELECT 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
                'E104' AS 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

             left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
    AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
    ON atn.date=tc.InPunchDay
            AND tc.Department='W104'

    WHERE atn.individual like 'WRIGHT%'
            AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




        

)
UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
                'E104' AS 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'

    FROM [attendance2022] atn

    left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>=30))
        
    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
            AND tc.Department='W104'

    WHERE atn.individual like 'WRIGHT%'
           AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3



        


)
UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End', 
                'E104' AS 'Home',
                'Teena' as 'Manager',
                'Kent County' as 'County'
    FROM [attendance2022] atn

          left Join isp
    ON (atn.date=isp.date)
    AND (isp.Individual=atn.individual)
    AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
    ON (atn.date=tc.OutPunchDay)
            AND tc.Department='W104'

    WHERE atn.individual like 'WRIGHT%'
           AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay


HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3




        

)

UNION
-- NYEA GOLDSBERRY --

(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '8 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM 
    [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA8'
        

WHERE 
    atn.individual like 'GOLDS%'
    AND isp.isp_program is NULL
    -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
    --AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='07:00'
    AND cast(tc.InPunchTime as time)<='10:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3





)

UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '8 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'


FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

    Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
        AND tc.Department='SA8'

WHERE 
    atn.individual like 'GOLDS%'
    AND isp.isp_program is NULL


Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='14:00'
    AND cast(tc.InPunchTime as time)<='17:00'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3





)

UNION
(Select 
    atn.individual as 'Name',
    atn.Date as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    tc.InPunchTime as 'Shift Start', 
    '11:59 PM' as 'Shift End',
    '8 Nairn' as 'Home',
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
        AND (cast(isp.begin_time as time)<='11:30pm' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
        AND tc.Department='SA8'

WHERE 
    atn.individual like 'GOLDS%'
    AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3





)

UNION
(Select 
    atn.individual as 'Name',
    tc.OutPunchDay as 'Date',
    concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    datename(weekday, atn.date) as 'Weekday', 
    '12:00 AM' as 'Shift Start', 
    tc.OutPunchTime as 'Shift End',
    '8 Nairn' as 'Home', 
    'David' as 'Manager',
    'New Castle County' as 'County'

FROM [attendance2022] atn

    left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

    Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
        AND tc.Department='SA8'

WHERE atn.individual like 'GOLDS%'
AND isp.isp_program is NULL

Group By atn.date, tc.EarnHours,
 
    atn.individual,
    tc.InPunchTime,
    tc.Firstname,
    tc.Lastname,
    tc.OutPunchTime, 
    tc.InPunchDay,
    atn.attendance,
    tc.OutPunchDay

HAVING 
    cast(tc.InPunchTime as time)>='6pm'
      AND atn.attendance like '%12%'
    AND tc.EarnHours > 3


)
ORDER BY 1
"""
isp_data = pd.read_sql_query(mdq, con=engine)
print(isp_data)


# Appointment Issues Query
aiq = """
SELECT  
    individual,
    date,
    begin_time as 'Time',
    provider,
    specialty,
    apt_status,
    follow_up_date,
    CASE
        WHEN 
            (program='3 Nairn Ln' OR program='8 Nairn Ln')
            THEN 'David'
        WHEN
            (program='324 Broadstairs' OR program='Westover E104' OR program='13B Dartmouth - Castlebrook')
            THEN 'Teena'
        WHEN
            (program='Katrina 110' OR program='Cannon Mills - 101')
            THEN 'Whitney'
        END AS 'Manager',
    CASE 
        WHEN
            (program='3 Nairn Ln' OR program='8 Nairn Ln' OR program='13B Dartmouth - Castlebrook')
            THEN 'New Castle County'
        WHEN
            (program='324 Broadstairs' OR program='Westover E104' OR program='104 Katrina Way' OR program='Cannon Mills - 101' OR program='Katrina 110')
            THEN 'Kent County'
        END AS 'County'

FROM
    Appointments2022

WHERE
    (apt_status='Scheduled')
    OR
    (apt_status='Cancelled' AND (Comment is null))
    OR 
    (apt_status='Rescheduled' AND follow_up_date is null)
    OR
    (apt_status='Declined' AND (follow_up_date is null OR Comment is null))
    OR 
    (apt_status='Not Scheduled' AND (comment is null OR [Description] is null))"""
apt_data = pd.read_sql_query(aiq, con=engine)
print(apt_data)


# Attendance Points Query
apq = """
SELECT
    EE_Code as [Employee ID],
    concat(pt.FirstName, ' ', pt.LastName) as 'Staff',
    Count(pt.Points)  as 'Points',
    AVG(pt.Minutes_Points_Off) as 'Average Time Late'
    
FROM
    atnPoints pt
    Join CurrentDSP dsp 
    ON pt.EE_Code=dsp.Employee_Code

Where 
    ScheduledDate > '04/17/2022'
GROUP BY 
    EE_Code, 
    pt.FirstName,
	pt.LastName

Order by 
	ee_code
    """
ap_data = pd.read_sql_query(apq, con=engine)
print(ap_data)


# Corrective Action Query
pdf = """
SELECT
    wu.Employee_Code,
    concat(wu.Legal_Firstname, ' ', wu.Legal_Lastname) as 'Employee',
    wu.Creation_Date,
    wu.discussion_reason,
    wu.Discussion_Template,
    CASE 
        WHEN wu.discussion_template='Memo of Conversation'
        THEN 'Supervisor Comments'
        WHEN wu.discussion_template!='Memo of Conversation'
        THEN 'Description of Incident'
        END as 'Field Description',
    wu.field_answer,
    Count (distinct wu.[discussion_id]) as Count,
    cd.supervisor

FROM
    WriteUps wu

JOIN
    currentdsp cd 
    ON cd.employee_code=wu.Employee_Code

WHERE
    wu.field_description='Description of Incident'
    OR wu.field_description='Supervisor Comments'

GROUP BY 
    wu.Employee_Code,
    wu.Legal_Firstname,
    wu.Legal_Lastname,
    wu.Creation_Date,
    wu.discussion_reason,
    wu.discussion_template,
    wu.field_answer,
    cd.supervisor

HAVING 
    cd.supervisor is not null 

ORDER BY
    wu.Employee_Code
    """
#pdf_data = pd.read_sql_query(pdf, con=engine)
#print(pdf_data)

# MissingData Excel File
xlwriter = pd.ExcelWriter(fr"{save_path}\DataReport({date}).xlsx")
isp_data.to_excel(xlwriter, sheet_name="ISPs", index=False)
apt_data.to_excel(xlwriter, sheet_name="Apts", index=False)
ap_data.to_excel(xlwriter, sheet_name="Attendance_Points", index=False)
#pdf_data.to_excel(xlwriter, sheet_name="Performance_Discussion_Forms", index=False)
xlwriter.close()

# The Program takes a break here for you to review the data and clean it before completing the final steps to display,
# staff who are missing documentation and their counts

print("Take a second to review the documentation, and upload missing data files to Notion.")
print(" ")
step = int(input("""Are you ready to move on? 
    1 for Y, 2 for N.
    
    """))

# Display the staff who are currently missing documentation, and how many documentations are they missing

if step == 1:
    data = pd.read_excel(fr"{save_path}\DataReport({date}).xlsx")
    counts = data.groupby(data["Staff Name"])
    print(counts["Staff Name"].count())
elif step == 2:
    print("error")
