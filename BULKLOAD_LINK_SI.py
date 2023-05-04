import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime


pathImport = '\import'
fields = ['LINK_ID','NODE_A','PORT_A','NODE_B','PORT_B']
fields2 = ['Link ID','Node_A','port_A','Node_B','port_B']
columnsToClean = ['Link ID']
pathImportSI = os.getcwd() + pathImport
li = []
def processArchive(filename,filtrolabel,filtroValue,OP):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  fname = filename[len(pathImportSI)+1:-20]
  for i in filtroValue:
    iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
    df = pd.concat([chunk[(chunk[filtrolabel[0]].str.contains(i,na=False)) | (chunk[filtrolabel[1]].str.contains(i,na=False)) | (chunk[filtrolabel[2]].str.contains(i,na=False))] for chunk in iter_csv]) # WORKS
    #df = pd.concat([chunk[(chunk[filtrolabel[0]].isin([filtroValue[0]])) & (chunk[filtrolabel[1]].isin([filtroValue[1]])) & (chunk[filtrolabel[2]].isin(filtroValue2))] for chunk in iter_csv]) # WORKS
    li.append(df)
  frameSI = pd.concat(li, axis=0, ignore_index=True)
  frameSI = frameSI[fields] # ordering labels
  frameSI.columns = fields2   
  frameSI = frameSI.drop_duplicates()
  frameSI[columnsToClean] = ''
  frameSI.insert(0, 'OP', OP)
  csv_path = os.path.join(script_dir, 'export/'+'link_si'+'.csv')
  frameSI.to_csv(csv_path,index=False,header=True,sep=';',encoding="UTF-8")
  return frameSI

