import configuration
import pyodbc
import utils
import logging
from datetime import datetime as dt


username = configuration.login_details['username']
password = configuration.login_details['password']
server = configuration.login_details['server']
database = configuration.login_details['database']


def getconobj():
    class confi:
        def __init__(self,username,password,server,database):
            self.username = username
            self.password = password
            self.server = server
            self.database = database
    return confi
    
def create_connection():
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          f'Server={server};'
                          f'Database={database};'
                          f'UID={username};'
                          f'PWD={password};''Trusted_Connection=no;')
    cursor = conn.cursor()
 
    return conn,cursor

def close_conn(conn,cursor):
    conn.close()
    cursor.close()
    
    
def setlogger(logfile,log=logging.getLogger(__name__),level=logging.DEBUG,logformat="%(asctime)s %(filename)s:%(lineno)s (%(funcName)s) %(levelname)s :: %(message)s"):
        
        log_f = logging.FileHandler(logfile)
        #log_f = logging.FileHandler(f'{data_dir}/{__file__}.log')
        log_f.setLevel(level)
        log_f.setFormatter(logformat)
        log.addHandler(log_f)
        return log
    
    
    
def null_handling(df):
    for columns in df.columns:
        if isinstance(df[columns].dtype,int):
            df.fillna(101)
        if isinstance(df[columns].dtype,str):
            df.fillna('N/A')
        #if isinstance(df[columns].dtype,dt.datetime.datetime):
            #df.fillna('01/01/1900')
        if isinstance(df[columns].dtype,float):
            df.fillna(101.00)
    return df
            
                  
                  
                  --------------------------------
                  mport pandas as pd
from datetime import datetime as dt
import numpy as np
import importlib
importlib.reload(utils)
import logging


def main():
     conn,cursor = utils.create_connection()
     logger.info('connect created')
     charge_quary = ''' select * from [BCMPWMT].CHARGE_CATEG_LKP'''
     dim_charge_quary = pd.read_sql(charge_quary,conn)
    
     logger.info('Query executed and src data extracted')
     dim_charge_quary['CHARGE_CATEG_ID']=dim_charge_quary['CHARGE_CATEG_ID'].str.strip().astype(int)
     dim_charge_quary['TENANT_ORG_ID'] = dim_charge_quary['TENANT_ORG_ID'].str.strip().astype(int)
     dim_charge_quary['CHARGE_CATEG'] = dim_charge_quary['CHARGE_CATEG'].apply(lambda x: x.strip() if len(x)>5  else str.upper(x.strip()))
     dim_charge_quary['CHARGE_CATEG_DESC'] = dim_charge_quary['CHARGE_CATEG_DESC'].str.strip()
     dim_charge_quary['TAX_IND']=dim_charge_quary['TAX_IND'].str.strip().astype(int)
     
     
     
     cleaned_df=utils.null_handling(dim_charge_quary)
     truncate_table ='''truncate table STG_DIM_CHARGE_CATEG_PYTHON_IN1548'''
     conn.execute(truncate_table)
     conn.commit()
     insertstmt=''
     for index,row in cleaned_df.iterrows():
       
         insertstmt+=f'''insert into STG_DIM_CHARGE_CATEG_PYTHON_IN1548
        values ({row['CHARGE_CATEG_ID']},{row['TENANT_ORG_ID']},'{row['CHARGE_CATEG']}','{row['CHARGE_CATEG_DESC']}', {row['TAX_IND']},1)
        '''
         print(insertstmt)
  
     cursor.execute(insertstmt)
     conn.commit()
     scd1_quary = '''INSERT INTO DIM_CHARGE_CATEG_PYTHON_IN1548
                        SELECT s.CHARGE_CATEG_ID
                        , s.TENANT_ORG_ID
                        , s.CHARGE_CATEG
                        , s.CHARGE_CATEG_DESC
                        , s.TAX_IND,
                        CASE
                        WHEN t.CHARGE_CATEG_ID IS NULL THEN 1
                        ELSE 1+(SELECT MAX(t.Version) FROM DIM_CHARGE_CATEG_PYTHON_IN1548 t JOIN STG_DIM_CHARGE_CATEG_PYTHON_IN1548 s ON s.CHARGE_CATEG_ID = t.CHARGE_CATEG_ID WHERE t.CHARGE_CATEG <> s.CHARGE_CATEG) END as Version
                        FROM STG_DIM_CHARGE_CATEG_PYTHON_IN1548 s
                        LEFT JOIN DIM_CHARGE_CATEG_PYTHON_IN1548 t
                        ON t.CHARGE_CATEG_ID = s.CHARGE_CATEG_ID
                        LEFT JOIN
                        (SELECT CHARGE_CATEG_ID, MAX(Version) as Max_Version from DIM_CHARGE_CATEG_PYTHON_IN1548 GROUP BY CHARGE_CATEG_ID) a
                        on t.CHARGE_CATEG_ID=a.CHARGE_CATEG_ID
                        WHERE t.CHARGE_CATEG_ID IS NULL OR ((t.CHARGE_CATEG_ID IS NOT NULL) AND (t.CHARGE_CATEG <> s.CHARGE_CATEG ) AND 
                        t.Version = a.Max_Version)
                        '''
                        
     cursor.execute(scd1_quary)
     conn.commit()
     
if __name__='__main__':
    main()
    -----------------------------
         
     
CREATE TABLE STG_DIM_CHARGE_CATEG_PYTHON_IN1548(
CHARGE_CATEG_KEY	INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
CHARGE_CATEG_ID	INT	NOT NULL,
TENANT_ORG_ID	INT	NOT NULL,
CHARGE_CATEG	VARCHAR(50) NOT NULL,
CHARGE_CATEG_DESC	VARCHAR(50)	NOT NULL,
TAX_IND	INT	NOT NULL,
VERSION	INT	
)


CREATE TABLE DIM_CHARGE_CATEG_PYTHON_IN1548(
CHARGE_CATEG_KEY	INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
CHARGE_CATEG_ID	INT	NOT NULL,
TENANT_ORG_ID	INT	NOT NULL,
CHARGE_CATEG	VARCHAR(50) NOT NULL,
CHARGE_CATEG_DESC	VARCHAR(50)	NOT NULL,
TAX_IND	INT	NOT NULL,
VERSION	INT	
)


update STG_DIM_CHARGE_CATEG_PYTHON_IN1548 
set charge_categ='fff'
where charge_categ_id = 31

select * from STG_DIM_CHARGE_CATEG_PYTHON_IN1548

drop table DIM_CHARGE_CATEG_PYTHON_IN1548

INSERT INTO DIM_CHARGE_CATEG_PYTHON_IN1548
SELECT s.CHARGE_CATEG_ID
, s.TENANT_ORG_ID
, s.CHARGE_CATEG
, s.CHARGE_CATEG_DESC
, s.TAX_IND,
CASE
WHEN t.CHARGE_CATEG_ID IS NULL THEN 1
ELSE 1+(SELECT MAX(t.Version) FROM DIM_CHARGE_CATEG_PYTHON_IN1548 t JOIN STG_DIM_CHARGE_CATEG_PYTHON_IN1548 s ON s.CHARGE_CATEG_ID = t.CHARGE_CATEG_ID WHERE t.CHARGE_CATEG <> s.CHARGE_CATEG) END as Version
FROM STG_DIM_CHARGE_CATEG_PYTHON_IN1548 s
LEFT JOIN DIM_CHARGE_CATEG_PYTHON_IN1548 t
ON t.CHARGE_CATEG_ID = s.CHARGE_CATEG_ID
LEFT JOIN
(SELECT CHARGE_CATEG_ID, MAX(Version) as Max_Version from DIM_CHARGE_CATEG_PYTHON_IN1548 GROUP BY CHARGE_CATEG_ID) a
on t.CHARGE_CATEG_ID=a.CHARGE_CATEG_ID
WHERE t.CHARGE_CATEG_ID IS NULL OR ((t.CHARGE_CATEG_ID IS NOT NULL) AND (t.CHARGE_CATEG <> s.CHARGE_CATEG ) AND 
t.Version = a.Max_Version)

select * from DIM_CHARGE_CATEG_PYTHON_IN1548

delete from DIM_CHARGE_CATEG_PYTHON_IN1548
