from imghdr import tests
import os, sys
from API import *
from structure import *


if __name__ == '__main__':
    
    col_names = ['ID','name','grade','dept'] # age 추가할수도?
    col_types = ['c8','v16','c1','v17']  # vlr 하나에 40bytes 이므로
    table_name = 'test10'

    # 기능 1
    createTable(table_name,col_names,col_types) 

    testslp = SLP()
    directory = os.getcwd() + '/table/' + table_name
    # with open(directory+'/slot1.bin', 'rb') as f:
    #     print(bytearray(f.read())  )
    
    # 기능 2
    '''
    insert into table_name
    values ('덕배', 2, 'db2', '010-1234-5678', '감기')
    '''
    insert_columns = ['20194653', 'dain', '4', 'sci']
    insertColumn(table_name, insert_columns)
    testslp.getSLP(table_name,1)
    testslp.printSLP()
    print('\n')
    insert_columns = ['20192019', 'yoon', '1', 'bio']
    insertColumn(table_name, insert_columns)
    testslp.getSLP(table_name,1)
    testslp.printSLP()
    print('\n')
    insert_columns = ['11112019', 'YDI', '1', 'hel']
    insertColumn(table_name, insert_columns)
    testslp.getSLP(table_name,1)
    testslp.printSLP()
    print('\n')
    #testvlr = VLR(table_name,insert_columns)


    # 기능 3
    query = 'select name from student where name = \'dain\''
    #findRecord(table_name, query)

    # 기능 4
    # col = findColumn(table_name)
    # print(f'columns for \"{table_name}\" table are : {col}')
    

    # test #
    #new = SLP()
    #print(new.slp)
    # new = SLP()
    # with open('slot7.bin', 'wb+') as f:
    #     f.write(new.slp)
    #     #print(len(f.readlines()))
        

    res = findRecord(select = 'name', tableName = table_name,  where = 'grade = 4')
    if (len(res) == 0) : print('no query')

    