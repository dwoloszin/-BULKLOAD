import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime


pathImport = '\import'
fields = ['MS_NAME','SI_PORT_NAME','CELL_SECTOR_NAME','PROVISION_STATUS','CELL_SECTOR_TYPE','CGI','PWSET','ACTIVE','COVERAGE','CELL_DONATOR','TRX_ID_1_ARFCN','TRX_ID_1_BCCH','TRX_ID_2_ARFCN','TRX_ID_2_BCCH','TRX_ID_3_ARFCN','TRX_ID_3_BCCH','TRX_ID_4_ARFCN','TRX_ID_4_BCCH','TRX_ID_5_ARFCN','TRX_ID_5_BCCH','TRX_ID_6_ARFCN','TRX_ID_6_BCCH','TRX_ID_7_ARFCN','TRX_ID_7_BCCH','TRX_ID_8_ARFCN','TRX_ID_8_BCCH','TRX_ID_9_ARFCN','TRX_ID_9_BCCH','TRX_ID_10_ARFCN','TRX_ID_10_BCCH','TRX_ID_11_ARFCN','TRX_ID_11_BCCH','TRX_ID_12_ARFCN','TRX_ID_12_BCCH']
fields2 = ['MS_Name','SI Port Name','Cell Sector Name','Provision Status','Cell Sector Type','CGI','PwSet(W)','Active','Coverage','Cell Donator','TRX_ID 1 ARFCN','TRX_ID 1 BCCH','TRX_ID 2 ARFCN','TRX_ID 2 BCCH','TRX_ID 3 ARFCN','TRX_ID 3 BCCH','TRX_ID 4 ARFCN','TRX_ID 4 BCCH','TRX_ID 5 ARFCN','TRX_ID 5 BCCH','TRX_ID 6 ARFCN','TRX_ID 6 BCCH','TRX_ID 7 ARFCN','TRX_ID 7 BCCH','TRX_ID 8 ARFCN','TRX_ID 8 BCCH','TRX_ID 9 ARFCN','TRX_ID 9 BCCH','TRX_ID 10 ARFCN','TRX_ID 10 BCCH','TRX_ID 11 ARFCN','TRX_ID 11 BCCH','TRX_ID 12 ARFCN','TRX_ID 12 BCCH']
columnsToClean = []
pathImportSI = os.getcwd() + pathImport
li = []
def processArchive(filename,filtrolabel,filtroValue,OP):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  fname = filename[len(pathImportSI)+1:-20]

  iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
  df = pd.concat([chunk for chunk in iter_csv])
  li.append(df)
  frameSI = pd.concat(li, axis=0, ignore_index=True)
  frameSI = frameSI[fields] # ordering labels 
  frameSI = frameSI.loc[frameSI[filtrolabel].isin(filtroValue)] 
  frameSI.columns = fields2
   
  frameSI = frameSI.drop_duplicates()
  frameSI[columnsToClean] = ''
  frameSI.insert(0, 'OP', OP)
  csv_path = os.path.join(script_dir, 'export/'+'cellsector'+'.csv')
  frameSI.to_csv(csv_path,index=False,header=True,sep=';',encoding="UTF-8")
  return frameSI

