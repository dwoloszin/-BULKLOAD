import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime


pathImport = '\import'
fields = ['LOCATION']
fields2 = ['LOCATION']
columnsToClean = []
pathImportSI = os.getcwd() + pathImport
li = []
def processArchive(filename):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  fname = filename[len(pathImportSI)+1:-20]
  iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
  #df = pd.concat([chunk[(chunk[filtrolabel].str.contains(i,na=False))] for chunk in iter_csv]) # WORKS
  df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS
  li.append(df)
  frameSI = pd.concat(li, axis=0, ignore_index=True)
  frameSI = frameSI[fields] # ordering labels
  frameSI.columns = fields2  
  frameSI = frameSI.drop_duplicates()
  return frameSI

