import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime


pathImport = '\import'
fields = ['LOCATION','LATITUDE','LONGITUDE','IBGE_ID','MOBILE_SITE_NAME','PROVISIONSTATUS','SITE_NAME','PROJETO','REAL_ACTIVATION_DATE','SITE_TYPE','IMPLEMENTATIION_STATUS','PROJECT_YEAR','SITE_COMMENTS','SHELTER_CONTAINER','DEVICE_CONFIGURATION','ANTENA_SYS_CLASS','INSTALL']
fields2 = ['Location','Latitude','Longitude','IBGE_ID','Mobile Site Name','Provision Status','Site Name','Project','Real Activation Date','Site Type','Implementation Status','Project Year','Site Comments','Shelter Container','Device Configuration','Antenna System Classification','Install']
columnsToClean = ['Latitude','Longitude','IBGE_ID','Real Activation Date']
pathImportSI = os.getcwd() + pathImport

def processArchive(filename,filtrolabel,filtroValue,OP):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  fname = filename[len(pathImportSI)+1:-20]
  li = []
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
  csv_path = os.path.join(script_dir, 'export/'+'mobilesite'+'.csv')
  frameSI.to_csv(csv_path,index=False,header=True,sep=';',encoding="UTF-8")
  return frameSI

