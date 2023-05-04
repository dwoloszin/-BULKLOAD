import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime


pathImport = '\import'
fields = ['SI_NAME','SI_PS','SI_PORT','SI_PORTA_STATUS','LOCATION','LOCATION_TYPE','MOBILE_SITE_NAME','LATITUDE','LONGITUDE','DT_ATIV_MOBILE_SITE','REGIONAL','UF','IBGE_ID','ANF','MUNICIPIO','CS_NAME','CS_STATUS','BSC_RNC','ANTENA_NAME','ANTENA_STATUS','AZIMUTH','ALTURA','MECHANICAL_TILT','ANTENA_MODEL','PORTA_ANTENA','STATUS_PORTA','RET','TIPO_CONECTOR','AMD_NAME','AMD_BAND','AMD_TILT_E','MS_TYPE','MS_PS','RF_DEVICE1','RF_DEVICE_MODEL1','RF_DEVICE2','RF_DEVICE_MODEL2','SI_PORT_BAND','SI_PORT_EIRP','CABO_1_2_COBRE','CABO_1_2_ALUMINIO','CABO_7_8_COBRE','CABO_7_8_ALUMINIO','CABO_1_1_4_COBRE','CABO_1_1_4_ALUMINIO','CABO_1_5_8_COBRE','CABO_1_5_8_ALUMINIO','COMP_TOTAL_JUMP_1_2_COBRE','COMP_TOTAL_JUMP_1_2_ALUMINIO','COMP_TOTAL_JUMP_7_8_COBRE','COMP_TOTAL_JUMP_78_ALUMINIO','PERDA_DOS_CONECTORES','PERDA_DE_POLARIZACAO','PERDA_DEV1','PERDA_DEV2','PERDA_TOTAL_DA_LINHA','REGIONAL_SEQUENCE','CST_NAME']
fields2 = ['SI_NAME','SI_PS','SI_PORT','SI_PORTA_STATUS','LOCATION','LOCATION_TYPE','MOBILE_SITE_NAME','LATITUDE','LONGITUDE','DT_ATIV_MOBILE_SITE','REGIONAL','UF','IBGE_ID','ANF','MUNICIPIO','CS_NAME','CS_STATUS','BSC_RNC','ANTENA_NAME','ANTENA_STATUS','AZIMUTH','ALTURA','MECHANICAL_TILT','ANTENA_MODEL','PORTA_ANTENA','STATUS_PORTA','RET','TIPO_CONECTOR','AMD_NAME','AMD_BAND','AMD_TILT_E','MS_TYPE','MS_PS','RF_DEVICE1','RF_DEVICE_MODEL1','RF_DEVICE2','RF_DEVICE_MODEL2','SI_PORT_BAND','SI_PORT_EIRP','CABO_1_2_COBRE','CABO_1_2_ALUMINIO','CABO_7_8_COBRE','CABO_7_8_ALUMINIO','CABO_1_1_4_COBRE','CABO_1_1_4_ALUMINIO','CABO_1_5_8_COBRE','CABO_1_5_8_ALUMINIO','COMP_TOTAL_JUMP_1_2_COBRE','COMP_TOTAL_JUMP_1_2_ALUMINIO','COMP_TOTAL_JUMP_7_8_COBRE','COMP_TOTAL_JUMP_78_ALUMINIO','PERDA_DOS_CONECTORES','PERDA_DE_POLARIZACAO','PERDA_DEV1','PERDA_DEV2','PERDA_TOTAL_DA_LINHA','REGIONAL_SEQUENCE','CST_NAME']
columnsToClean = []
pathImportSI = os.getcwd() + pathImport

def processArchive(filename,filtrolabel,filtroValue,OP):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  fname = filename[len(pathImportSI)+1:-20]
  li = []
  for i in filtroValue:
    iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
    df = pd.concat([chunk[(chunk[filtrolabel].str.contains(i,na=False))] for chunk in iter_csv]) # WORKS
    li.append(df)
  frameSI = pd.concat(li, axis=0, ignore_index=True)
  frameSI = frameSI[fields] # ordering labels
  frameSI.columns = fields2  
  frameSI = frameSI.drop_duplicates()
  frameSI[columnsToClean] = ''
  frameSI.insert(0, 'OP', OP)
  csv_path = os.path.join(script_dir, 'export/export/'+'REPORT_SI_SI'+'.csv')
  frameSI.to_csv(csv_path,index=False,header=True,sep=';',encoding="UTF-8")
  return frameSI

