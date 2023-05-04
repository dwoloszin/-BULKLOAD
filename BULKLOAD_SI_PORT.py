import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime


pathImport = '\import'
fields = ['SI_NAME','SI_PORT_NAME','PORT_STATUS','SI_PORT_BAND','SI_PORT_EIRP','CABO_1_2_COBRE','CABO_1_2_ALUMINIO','CABO_7_8_COBRE','CABO_7_8_ALUMINIO','CABO_1_1_4_COBRE','CABO_1_1_4_ALUMINIO','CABO_1_5_8_COBRE','CABO_1_5_8_ALUMINIO','COMP_TOTAL_JUMP_1_2_COBRE','COMP_TOTAL_JUMP_1_2_ALUMINIO','COMP_TOTAL_JUMP_7_8_COBRE','COMP_TOTAL_JUMP_78_ALUMINIO','PERDA_DOS_CONECTORES','PERDA_DE_POLARIZACAO','PERDA_TOTAL_DA_LINHA']
columnsToClean = []
pathImportSI = os.getcwd() + pathImport
li = []
def processArchive(filename,filtrolabel,filtroValue,OP):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  fname = filename[len(pathImportSI)+1:-20]
  for i in filtroValue:
    iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
    df = pd.concat([chunk[(chunk[filtrolabel].str.contains(i,na=False))] for chunk in iter_csv]) # WORKS
    li.append(df)
  frameSI = pd.concat(li, axis=0, ignore_index=True)
  frameSI = frameSI[fields] # ordering labels  
  frameSI = frameSI.drop_duplicates()
  frameSI[columnsToClean] = ''
  frameSI.insert(0, 'OP', OP)
  csv_path = os.path.join(script_dir, 'export/'+'siport'+'.csv')
  frameSI.to_csv(csv_path,index=False,header=True,sep=';',encoding="UTF-8")
  return frameSI

