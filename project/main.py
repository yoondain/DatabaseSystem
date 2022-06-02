from API import *
from structure import *


if __name__ == '__main__':
    
    col_names = ['ID','name','grade','dept'] # age 추가할수도?
    col_types = ['c8','v16','c1','v17']  # vlr 하나에 40bytes 이므로
    table_name = 'test'

    

    testslp = SLP()
    directory = os.getcwd() + '/table/' + table_name


    
   

    # # testslp.getSLP(table_name,1)
    # # testslp.printSLP()
    # # with open(directory+'/slot1.bin', 'rb') as f:
    # #     print(bytearray(f.read())  )
    


    # # 기능 2
    # '''
    # insert into table_name
    # values ('덕배', 2, 'db2', '010-1234-5678', '감기')
    # '''
    # insert_columns = ['20194653', 'dain', '4', 'sci']
    # insertColumn(table_name, insert_columns)
    # testslp.getSLP(table_name,1)
    # testslp.printSLP()
    # print('\n')
    # insert_columns = ['20192019', 'yoon', 'null', 'bio']
    # insertColumn(table_name, insert_columns)
    # testslp.getSLP(table_name,1)
    # testslp.printSLP()
    # print('\n')
    # insert_columns = ['null', 'YDI', '1', 'hel']
    # insertColumn(table_name, insert_columns)
    # testslp.getSLP(table_name,1)
    # testslp.printSLP()
    # print('\n')


    # testvlr = VLR()
    # testvlr.makeVLR(table_name,insert_columns)

    
    # print('\n')
    # 기능 3
    # with open(directory+'/slot1.bin', 'rb') as f:
    #     print(bytearray(f.read()))
    

    # query = 'select name from student where name = \'dain\''


    # # 기능 1
    createTable(table_name,col_names,col_types) 
    # # 현재 3개 inserted
    insert_columns = ['20194653', 'dain', '1', 'sci']
    insertColumn(table_name, insert_columns)
    insert_columns = ['00000000', 'yoon', '3', 'null'] # grade 값이 null
    insertColumn(table_name, insert_columns)
    insert_columns = ['20194653', 'YDI', '1', 'sci']
    insertColumn(table_name, insert_columns)


    # =======

    insert_columns = ['12345678', 'my name', '4', 'math']
    insertColumn(table_name, insert_columns)



    testslp.getSLP(table_name,1)
    testslp.printSLP()
    print('\n')
    testslp.getSLP(table_name,2)
    testslp.printSLP()
    print('\n')


    # res = findRecord(select = 'name', 
    #                 tableName = table_name,  
    #                 where = 'dept', 
    #                 target = 'sci')
    # if (len(res) == 0) : print('query not found')

    # insert_columns = ['20194653', 'dain', '1', 'sci']
    # testvlr = VLR()
    # testvlr.makeVLR(table_name,insert_columns)

    # tempbytearr = VLR()
    # tempbytearr.makeVLR_bytearray(testvlr.vlr,table_name)
    # tempbytearr.printVLR()