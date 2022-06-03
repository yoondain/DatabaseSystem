from API import *
from structure import *


if __name__ == '__main__':
    
    table_name = 'student'
    col_names = ['ID','name','grade','dept'] 
    col_types = ['c8','v16','c1','v17'] 
    

    

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
    # insert_record = ['20194653', 'dain', '4', 'sci']
    # insertRecord(table_name, insert_record)
    # testslp.getSLP(table_name,1)
    # testslp.printSLP()
    # print('\n')
    # insert_record = ['20192019', 'yoon', 'null', 'bio']
    # insertRecord(table_name, insert_record)
    # testslp.getSLP(table_name,1)
    # testslp.printSLP()
    # print('\n')
    # insert_record = ['null', 'YDI', '1', 'hel']
    # insertRecord(table_name, insert_record)
    # testslp.getSLP(table_name,1)
    # testslp.printSLP()
    # print('\n')


    # testvlr = VLR()
    # testvlr.makeVLR(table_name,insert_record)

    
    # print('\n')
    # 기능 3
    # with open(directory+'/slot1.bin', 'rb') as f:
    #     print(bytearray(f.read()))
    

    # query = 'select name from student where name = \'dain\''


    # 기능 1
    createTable(table_name,col_names,col_types) 


    # # 현재 3개 inserted
    insert_record = ['20194653', 'dain', '1', 'sci']
    insertRecord(table_name, insert_record)
    insert_record = ['00000000', 'yoon', '3', 'null'] # grade 값이 null
    insertRecord(table_name, insert_record)
    insert_record = ['20194653', 'YDI', '1', 'sci']
    insertRecord(table_name, insert_record)


    # =======

    insert_record = ['12345678', 'my name', '4', 'math']
    insertRecord(table_name, insert_record)



    metadata = dataDict()
    metadata.getDict(table_name)

    for i in range(1, metadata.slotNum+1):
        pass
        testslp.getSLP(table_name,i)
        testslp.printSLP()
        print('\n')

    # testslp.getSLP(table_name,2)
    # testslp.printSLP()
    # print('\n')


    # res = findRecord(select = 'name', 
    #                 table_name = table_name,  
    #                 where = 'dept', 
    #                 target = 'sci')
    # if (len(res) == 0) : print('query not found')

    # insert_record = ['20194653', 'dain', '1', 'sci']
    # testvlr = VLR()
    # testvlr.makeVLR(table_name,insert_record)

    # tempbytearr = VLR()
    # tempbytearr.makeVLR_bytearray(testvlr.vlr,table_name)
    # tempbytearr.printVLR()