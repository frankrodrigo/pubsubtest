import pyodbc

# SQL Server Configuration
server = '162.222.181.70'
database = 'db1'
username = 'dbuser1'
password = 'dbuser1'
driver = '{ODBC Driver 17 for SQL Server}'

# Connect to SQL Server
try:
    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(connection_string)
    print("Connection successful!")
    conn.close()
except Exception as e:
    print("Error connecting to database:", e)
