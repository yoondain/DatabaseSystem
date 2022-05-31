import os, sys
from API import *
from structure import *


if __name__ == '__main__':
    
    col_names = ['ID','name','grade','dept']
    col_types = ['c5','v8','c1','v10'] 
    table_name = 'test2'
    # createTable('test5',col_names,col_types )    
    createTable(table_name,col_names,col_types ) 
    # temp.makeDic(table_name, col_names, col_types)
    temp = dataDict()
    temp.getDict(table_name)
    # temp.printDict()
    #findColumn(table_name)


    '''
    insert into table_name
    values ('덕배', 2, 'db2', '010-1234-5678', '감기')
    '''
    
    table_name = table_name
    insert_columns = ['11111', 'dain', '4', 'comp sci']
    tempvlr = VLR(table_name, insert_columns)
    pass
