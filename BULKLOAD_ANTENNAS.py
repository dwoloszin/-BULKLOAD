import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime


pathImport = '\import'
fields = ['LOCATION','ANTENNA_MODEL_NAME','NAME','PROVISIONING_STATUS','DEVICE_TYPE','DEVICE_SUBTYPE','AZIMUTE','ALTURA','TILT_MECANICO']
fields2 = ['Location','Antenna Model.Name','Name','Provision Status','Device Type','Device SubType','Azimute','Altura (m)','Tilt Mecanico (Graus)']
columnsToClean = []
pathImportSI = os.getcwd() + pathImport
li = []
def processArchive(filename,filtrolabel,filtroValue,OP):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  fname = filename[len(pathImportSI)+1:-20]
  for i in filtroValue:
    iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
    df = pd.concat([chunk[(chunk[filtrolabel].str.contains(i))] for chunk in iter_csv]) # WORKS
    li.append(df)
  frameSI = pd.concat(li, axis=0, ignore_index=True)
  frameSI = frameSI[fields] # ordering labels
  frameSI.columns = fields2   
  frameSI = frameSI.drop_duplicates()
  frameSI[columnsToClean] = ''
  frameSI.insert(0, 'OP', OP)
  frameSI = frameSI.sort_values(['Name'], ascending = [True])
  csv_path = os.path.join(script_dir, 'export/'+'antenna'+'.csv')
  frameSI.to_csv(csv_path,index=False,header=True,sep=';',encoding="UTF-8")
  return frameSI

