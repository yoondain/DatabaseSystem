import os, sys
from structure import *
from bitstring import BitArray
from API import *


'''
하나의 talbe -> 하나의 folder
'''
def createTable(tableName:str, col_name : list, col_type:list):

    # folder 만들 위치
    directory = os.getcwd() + '/table/' + tableName
    try:
        
        if not os.path.exists(directory):
            # print('make')
            os.makedirs(directory)
        else:
            pass
            #print(f'\"{tableName}\" table already exists')
            # exit()
    except OSError:
        print(r'Error: Creating directory. ' +  directory)
        exit()

    
    metadata = dataDict()
    metadata.makeDict(tableName,col_name, col_type)
    pass

# =====================================================

def insertColumn(tableName:str, col_name : list, col_type:list):
    newRecord = VLR()
    pass

# =====================================================
def findRecord(query : str):
    pass


# =====================================================
def findColumn(tableName: str): # table name 받으면 해당 table 에 있는 column을 return
    meta_data = dataDict()
    meta_data.getDict(tableName)
    return meta_data.colName
