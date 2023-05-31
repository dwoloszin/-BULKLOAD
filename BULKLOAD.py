import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime
import BULKLOAD_MOBILESITE
import BULKLOAD_ANTENNA_PORT
import BULKLOAD_ANTENNAS
import BULKLOAD_CELLSECTOR
import BULKLOAD_LINK_SI
import BULKLOAD_RFDEVICE
import BULKLOAD_SI
import BULKLOAD_SI_PORT
import REPORT_SI_SI
import SITE_LIST





pathImport = '\import'
OP = 'Update' #'Update|Insert'

def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  pathImportSI = os.getcwd() + pathImport
  all_filesSI = glob.glob(pathImportSI + "/*.csv")
  all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
  list_text = []
  filtroValue = []
  oneVex = True
  for filename in all_filesSI:
    fname = filename[len(pathImportSI)+1:-20]
    if fname == 'SITE_LIST': #Need to save MobileSite last
      ID_LIST = SITE_LIST.processArchive(filename)
      ID_LIST.name = 'ID_LIST'
      ID_LIST = change_columnsName(ID_LIST)
      filtroValue = ID_LIST['LOCATION_ID_LIST'].tolist()
      print(fname,' Done!')
  
  
  #CellMobile(all_filesSI,pathImportSI,filtroValue)


  
    if fname == 'BULKLOAD_ANTENNA_PORT':
      ATP = BULKLOAD_ANTENNA_PORT.processArchive(filename,'ANTENNA_NAME',filtroValue,OP)
      ATP.name = 'ATP'
      ATP = change_columnsName(ATP)
      print(fname,' Done!') 
    if fname == 'BULKLOAD_ANTENNAS':
      AT = BULKLOAD_ANTENNAS.processArchive(filename,'LOCATION',filtroValue,OP)
      AT.name = 'AT'
      AT = change_columnsName(AT)
      print(fname,' Done!') 

    if fname == 'BULKLOAD_LINK_SI':
      LK_SI = BULKLOAD_LINK_SI.processArchive(filename,['NODE_A','NODE_B','PORT_B'],filtroValue,OP)
      LK_SI.name = 'LK_SI'
      LK_SI = change_columnsName(LK_SI)
      print(fname,' Done!')
    if fname == 'BULKLOAD_RFDEVICE':
      RFD = BULKLOAD_RFDEVICE.processArchive(filename,'LOCATION',filtroValue,OP)#DEVICE
      RFD.name = 'RFD'
      RFD = change_columnsName(RFD)
      print(fname,' Done!') 
    if fname == 'BULKLOAD_SI':
      SI = BULKLOAD_SI.processArchive(filename,'NOME_SI',filtroValue,OP)
      SI.name = 'SI'
      SI = change_columnsName(SI)
      print(fname,' Done!')
    if fname == 'BULKLOAD_SI_PORT':
      SIP = BULKLOAD_SI_PORT.processArchive(filename,'SI_NAME',filtroValue,OP)
      SIP.name = 'SIP'
      SIP = change_columnsName(SIP)
      print(fname,' Done!')
    if fname == 'REPORT_SI_SI':
      REPORT_SI = REPORT_SI_SI.processArchive(filename,'LOCATION',filtroValue,OP)
      REPORT_SI.name = 'REPORT_SI'
      REPORT_SI = change_columnsName(REPORT_SI)
      print(fname,' Done!')
  
  MBS,CELLSEC = CellMobile(all_filesSI,pathImportSI,filtroValue)  
  
  
  frameSI = pd.merge(CELLSEC,SIP, how='outer',left_on=['SI Port Name_CELLSEC'],right_on=['SI_PORT_NAME_SIP'])
 
  frameSI = pd.merge(frameSI,SI, how='outer',left_on=['SI_NAME_SIP'],right_on=['SI.Name_SI'])

  frameSI = pd.merge(frameSI,MBS, how='outer',left_on=['MS_Name_CELLSEC'],right_on=['Mobile Site Name_MBS'])

  frameSI = pd.merge(frameSI,LK_SI, how='outer',left_on=['SI Port Name_CELLSEC'],right_on=['port_B_LK_SI'])

  frameSI = pd.merge(frameSI,RFD, how='outer',left_on=['Node_A_LK_SI'],right_on=['Name_RFD'])

  frameSI['ref1'] = frameSI['Node_A_LK_SI'].astype(str) + frameSI['port_A_LK_SI'].astype(str)
  ATP['ref2'] = ATP['Antena Name_ATP'].astype(str) + ATP['Antena Porta Name_ATP'].astype(str)

  frameSI = pd.merge(frameSI,ATP, how='outer',left_on=['ref1'],right_on=['ref2'])
  frameSI.loc[~frameSI['Name_RFD'].isna(),['Antena Name_ATP']] = frameSI['Name_RFD']

  frameSI = pd.merge(frameSI,AT, how='outer',left_on=['Antena Name_ATP'],right_on=['Name_AT'])








  csv_path = os.path.join(script_dir, 'export/export/'+'Consolidado'+'.csv')
  frameSI.to_csv(csv_path,index=False,header=True,sep=';',encoding="UTF-8")
  



def change_columnsName(df):
  for i in df.columns:
      df.rename(columns={i:i + '_' + df.name},inplace=True)
  return df  




def CellMobile(all_filesSI,pathImportSI,filtroValue):
  for filename in all_filesSI:
    fname = filename[len(pathImportSI)+1:-20]
    if fname == 'BULKLOAD_MOBILESITE': #Need to save MobileSite last
      MBS = BULKLOAD_MOBILESITE.processArchive(filename,'LOCATION',filtroValue,OP)
      MBS.name = 'MBS'
      MBS = change_columnsName(MBS)
      list_text = MBS['Mobile Site Name_MBS'].tolist()
      print(fname,' Done!')


  for filename in all_filesSI:
    fname = filename[len(pathImportSI)+1:-20] 
    if fname == 'BULKLOAD_CELLSECTOR':
      CELLSEC = BULKLOAD_CELLSECTOR.processArchive(filename,'MS_NAME',list_text,OP) #Need to save MobileSite last
      CELLSEC.name = 'CELLSEC'
      CELLSEC = change_columnsName(CELLSEC)
      print(fname,' Done!')

  return MBS,CELLSEC