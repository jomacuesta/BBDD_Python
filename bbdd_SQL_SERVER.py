# -*- coding: utf-8 -*-
"""
Automate Changes in database
"""

import pyodbc 
import pandas as pd
import os
import urllib
from sqlalchemy import create_engine


PathFolder = "name_path"
PathFile = "name_path"
table = "name_table"

############################# LIBRARY OF FUNCTIONS ###########################


def connectDB_Azure():

	""" Formato 1 Conexión Azure SQL SERVER """
    
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=server_name_azure;'
                      'Database=name_database;'
                      'Trusted_Connection=yes;')

    cursor = conn.cursor()
    
    
    return cursor

def connectDB_Azure2():

	""" Formato 2 Conexión Azure SQL SERVER """
    
    server = 'server_name_azure'
    database = 'name_database'
    username = 'user'
    password = 'password'
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    
    return cnxn


def appendFiles(pathFolder):
 
    files = os.listdir(pathFolder)
    df = pd.concat([pd.read_csv(pathFolder + file) for file in files ])
        
    return df



def insertToTable(database,table,df):
    
   params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=server_string;DATABASE=%s;Trusted_Connection=Yes" % database)
   engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
   df.to_sql(table, engine, if_exists='append')
    
def insert(df):

   database = "database_name"
   table = "table_name"
   params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=server_string;DATABASE=%s;Trusted_Connection=Yes" % database)
   engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
   df.to_sql(table, engine, if_exists='append') #Si existe la tabla inserta los registros a los existentes
    
def updateTable(cursor,table,columnSet,setValue,columnWhere,whereValue):
    
    cursor.execute('''
                       UPDATE '%s'
                       SET '%s' = '%s'
                       WHERE '%s'  = '%s'
                       
                       ''' % (table,columnSet,setValue,columnWhere,whereValue))
    cursor.commit()
    cursor.close()
    
    
def execProc(cursor,storedProc,params):
    
    cursor.execute(storedProc,params)
    cursor.commit()
    cursor.close()
    
    
def createTable(cursor):
       cursor.execute('''
                      
           CREATE TABLE tab_test_python (   
                
               "Id_Empresa" NVARCHAR (255),
               "Nombre_Empresa" NVARCHAR (255),
               "Num_Empresa" NVARCHAR (255),
               "Ventas_2010" NVARCHAR (255)
               
          )
       ''') 
       cursor.commit()
    

###############################  MAIN   #################################

if __name__ == "__main__":

 
   conn = connectDB_Azure()
   cursor = conn.cursor()
   #---INSERT - Metodo 1 - DF completo
   df = pd.read_excel(PathFile)
   insert("ejemplo_database", "ejemplo_table_bbdd", df)
   
   #---INSERT - Metodo 2 - Row by Row
   conn = connectDB_Azure()
   createTable(cursor) #Paso opcional para crear la tabla
   
   for index, row in df.iterrows():
       print(index)
       cursor.execute("INSERT INTO tab_ejemplo VALUES " \
                      "(?,?,?,?,?,?,?,?,?,?)", str(row[0]), str(row[1]), str(row[2]),str(row[3]),str(row[4]))
       
   cursor.commit()
   cursor.close()
   
   #---READ TABLE
   df = pd.read_sql_query(''' SELECT * FROM tab_ejemplo''',conn)
   

        
