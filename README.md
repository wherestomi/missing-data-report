# missing-data-report
 Code to update site manager dashboards

Packages Needed 
- pandas
- openpyxl
- pyodbc
- sqlalchemy


Important variables;
- azure_cnxn.cnxn  | Entry into MS SQL Server
  - The azure_cnxn.py file
    contains a server, database, username, and password 
    variable that must be filled with your specific information to access your MS SQL Server Database
- main.save_path  |  path script for file storage
  - This will be the folder that houses the files created and exported through this script
- 