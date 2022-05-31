import os, sys
from structure import *
from bitstring import BitArray
from API import *


'''
하나의 talbe -> 하나의 folder
'''
def createTable(tableName:str, col_name : list, col_type:list):
    metadata = dataDict()
    metadata.makeDict(tableName,col_name, col_type)

# =====================================================

def insertColumn(tableName:str, col_name : list):
    newRecord = VLR(tableName,col_name)
    pass

# =====================================================
def findRecord(tableName:str, query : str):
    pass


# =====================================================
def findColumn(tableName: str): # table name 받으면 해당 table 에 있는 column을 return
    meta_data = dataDict()
    meta_data.getDict(tableName)
    return meta_data.colName
