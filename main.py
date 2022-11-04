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
save_path = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\Data_Pulls\11_November\{date}"
nc_isp_path = fr"{save_path}\RAW\nc_isp.xlsx"
kc_isp_path = fr"{save_path}\RAW\kc_isp.xlsx"
q1 = fr"{save_path}\RAW\q1atn.xlsx"
q2 = fr"{save_path}\RAW\q2atn.xlsx"
q3 = fr"{save_path}\RAW\q3atn.xlsx"
q4 = fr"{save_path}\RAW\q4atn.xlsx"
timecard_path = fr"{save_path}\RAW\timecards.csv"
apt_path = fr"{save_path}\RAW\apts.xlsx"
points_path = fr"{save_path}\RAW\atnpoints.csv"
pdf_path = fr"{save_path}\RAW\pdfs.csv"



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

cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
engine = sql.create_engine(cnxn_url)


# Missing Data Query
mdq = """  
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        'Castlebrook' as 'Home',
        'Paul' as 'Manager',
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


    Group By
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

    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
        'Castlebrook' as 'Home',
        'Paul' as 'Manager',
        'New Castle County' as 'County'

    FROM
        [Attendance2022] atn

            Left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
                    AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

            Left Join   TimeCards2022 tc
                ON (atn.date=tc.InPunchDay)
                AND tc.Department='13B Castlebrook'

    WHERE
        atn.individual like 'HEAD%'
        AND isp.isp_program is NULL

    Group By
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

        )

        UNION
        (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        'Castlebrook' as 'Home',
        'Paul' as 'Manager',
        'New Castle County' as 'County'

    FROM
        [Attendance2022] atn

            Left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)='12:00 AM' AND isp.[duration]>58))

            Left Join TimeCards2022 tc
                ON (atn.date=tc.OutPunchDay)
                AND tc.Department='13B Castlebrook'

    WHERE
        atn.individual like 'HEAD%'
        AND isp.isp_program is NULL

    Group By
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

    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        'Castlebrook' as 'Home',
        'Paul' as 'Manager',
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


    Group By
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

    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
        'Castlebrook' as 'Home',
        'Paul' as 'Manager',
        'New Castle County' as 'County'

    FROM
        [Attendance2022] atn

            Left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
                    AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

            Left Join   TimeCards2022 tc
                ON (atn.date=tc.InPunchDay)
                AND tc.Department='13B Castlebrook'

    WHERE
        atn.individual like 'FAUST%'
        AND isp.isp_program is NULL

    Group By
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

        )

        UNION
        (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        'Castlebrook' as 'Home',
        'Paul' as 'Manager',
        'New Castle County' as 'County'

    FROM
        [Attendance2022] atn

            Left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)='12:00 AM' AND isp.[duration]>58))

            Left Join TimeCards2022 tc
                ON (atn.date=tc.OutPunchDay)
                AND tc.Department='13B Castlebrook'

    WHERE
        atn.individual like 'FAUST%'
        AND isp.isp_program is NULL

    Group By
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

    )


    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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


    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
            AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA3'

    WHERE
        atn.individual like 'GARR%'
        AND isp.isp_program is NULL

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA3'

    WHERE atn.individual like 'GARR%'
        AND isp.isp_program is NULL

    Group By
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






    )



    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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


    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
            AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA3'

    WHERE
        atn.individual like 'LANI%'
        AND isp.isp_program is NULL

    Group By
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





    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA3'

    WHERE atn.individual like 'LANI%'
    AND isp.isp_program is NULL

    Group By
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






    )


    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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


    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
            AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA3'

    WHERE
        atn.individual like 'GALL%'
        AND isp.isp_program is NULL

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA3'

    WHERE atn.individual like 'GALL%'
    AND isp.isp_program is NULL

    Group By
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






    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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


    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
        '8 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
            AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA8'

    WHERE
        atn.individual like 'JARD%'
        AND isp.isp_program is NULL

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        '8 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA8'

    WHERE atn.individual like 'JARD%'
    AND isp.isp_program is NULL

    Group By
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






    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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






    )
    UNION
    (SELECT
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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






    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

        left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
                    AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
                AND tc.Department='W104'

        WHERE atn.individual like 'SEWARD%'
               AND isp.isp_program is NULL

    Group By
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






    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'
        FROM [attendance2022] atn

              left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
                AND tc.Department='W104'

        WHERE atn.individual like 'SEWARD%'
               AND isp.isp_program is NULL

    Group By
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






    )

    UNION
    (SELECT
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
                    'J101' as 'Home',
                    'Paul' as 'Manager',
                    'Kent County' as 'County'


        From TOBOLA..[isp] isp
                    Right Join TOBOLA..[attendance2022] atn
                        On atn.date=isp.[date] AND atn.individual=isp.individual
                    Right Join TimeCards2022 tc
                        On (concat(datename(weekday, tc.InPunchDay), ', ',datename(MONTH, tc.InPunchDay),' ', datename(day, tc.InPunchDay),', ', datename(year, tc.InPunchDay)))
        =
        concat(datename(weekday, atn.date), ', ',datename(MONTH, atn.date),' ', datename(day, atn.date),', ', datename(year, atn.date))

        Where (atn.Program_Site like '324%' or atn.Program_Site like '104%' or atn.Program_Site like '%101%' or atn.Program_Site like '%110%' or atn.Program_Site like 'west%')
                    AND atn.attendance like '%12%' AND isp.date is null
                    AND atn.individual like 'levan%'
                    AND tc.Department='J101'AND (tc.EarnCode='R' OR tc.EarnCode is null)


    Group By
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

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        'K110' as 'Home',
        'Paul' as 'Manager',
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

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        'K110' as 'Home',
        'Paul' as 'Manager',
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


    Group By
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



    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
        'K110' as 'Home',
        'Paul' as 'Manager',
        'Kent County' as 'County'

    FROM [Attendance2022] atn

        left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
        AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
        AND tc.Department='k110'

    WHERE
        atn.individual like 'gree%'
        AND isp.isp_program is NULL

    Group By
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



    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        'K110' as 'Home',
        'Paul' as 'Manager',
        'Kent County' as 'County'

    FROM [Attendance2022] atn

        left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
        AND tc.Department='k110'

    WHERE
        atn.individual like 'gree%'
        AND isp.isp_program is NULL


    Group By
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





    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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
    Group By
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







    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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


    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

             left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
                    AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
                AND tc.Department='W103'

        WHERE atn.individual like 'JAMES%'
                AND isp.isp_program is NULL

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

               left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
                AND tc.Department='W103'

        WHERE atn.individual like 'JAMES%'
               AND isp.isp_program is NULL


    Group By
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







    )


    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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
    Group By
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







    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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


    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

             left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
                    AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
                AND tc.Department='W103'

        WHERE atn.individual like 'CHIT%'
                AND isp.isp_program is NULL

    Group By
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





    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

               left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
                AND tc.Department='W103'

        WHERE atn.individual like 'CHIT%'
               AND isp.isp_program is NULL


    Group By
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







    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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


    Group By
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




    )


    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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






    )
    UNION
    (SELECT
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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






    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

        left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
                    AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
                AND tc.Department='W104'

        WHERE atn.individual like 'WRIGHT%'
               AND isp.isp_program is NULL

    Group By
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






    )
    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'
        FROM [attendance2022] atn

              left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
                AND tc.Department='W104'

        WHERE atn.individual like 'WRIGHT%'
               AND isp.isp_program is NULL

    Group By
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






    )

    UNION

    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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

    Group By
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





    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
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


    Group By
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





    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.InPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        tc.InPunchTime as 'Shift Start',
        '11:59 PM' as 'Shift End',
        '8 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>58)
            AND (cast(isp.begin_time as time)<='11pm' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA8'

    WHERE
        atn.individual like 'GOLDS%'
        AND isp.isp_program is NULL

    Group By
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





    )

    UNION
    (Select
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, tc.InPunchDay) as 'Weekday',
        '12:00 AM' as 'Shift Start',
        tc.OutPunchTime as 'Shift End',
        '8 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>58))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA8'

    WHERE atn.individual like 'GOLDS%'
    AND isp.isp_program is NULL

    Group By
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


    )
    """

result = pd.read_sql_query(mdq, con=engine)
print(result)
result.to_excel(fr"{save_path}\MissingData({date}).xlsx")
