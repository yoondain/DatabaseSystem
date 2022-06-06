from API import *
from structure import *


if __name__ == '__main__':
    
    table_name = 'student'
    col_names = ['ID','name','grade','dept'] 
    col_types = ['c8','v16','c1','v17'] 

    # metadata
    metadata = dataDict()
    # SLP
    slp = SLP()

    # 1 create table
    print("========create table===========")
    createTable(table_name,col_names,col_types) 
    print(f'create table "{table_name}"')
    metadata.getDict(table_name)
    print(f'# of SLP:{metadata.slotNum} , # of Record:{metadata.recordNum}')
    metadata.printDict()

    # 2 record insert 
    # slot page 의 사이즈에 딱 맞는 경우
    print("=========insert record 1===========")
    insert_record = ['20194653', 'dain', '1', 'sci']
    insertRecord(table_name, insert_record)
    metadata.getDict(table_name)
    print(f'# of SLP:{metadata.slotNum} , # of Record:{metadata.recordNum}')
    slp.getSLP(table_name,metadata.slotNum)
    slp.printSLP()

    print("=========insert record 2===========")
    insert_record = ['12345678', 'null', '1', 'sci']
    insertRecord(table_name, insert_record)
    metadata.getDict(table_name)
    print(f'# of SLP:{metadata.slotNum} , # of Record:{metadata.recordNum}')
    slp.getSLP(table_name,metadata.slotNum)
    slp.printSLP()

    print("=========insert record 3===========")
    insert_record = ['00000000', 'yoon', '3', 'null'] 
    insertRecord(table_name, insert_record)
    metadata.getDict(table_name)
    print(f'# of SLP:{metadata.slotNum} , # of Record:{metadata.recordNum}')
    slp.getSLP(table_name,metadata.slotNum)
    slp.printSLP()
    
    # 하나를 더 추가하면 slot의 개수가 늘어난다.
    print("=========insert record 4===========")
    insert_record = ['3333333', 'dd', '1', 'biology']
    insertRecord(table_name, insert_record)
    metadata.getDict(table_name)
    print(f'# of SLP:{metadata.slotNum} , # of Record:{metadata.recordNum}')
    slp.getSLP(table_name,metadata.slotNum)
    slp.printSLP()

    print("=========insert record 5===========")
    insert_record = ['13131313', 'null', '1', 'sci']
    insertRecord(table_name, insert_record)
    metadata.getDict(table_name)
    print(f'# of SLP:{metadata.slotNum} , # of Record:{metadata.recordNum}')
    slp.getSLP(table_name,metadata.slotNum)
    slp.printSLP()

    # 3. find record
    # =============== 특정 column ====================
    res = findRecord(select = 'ID', 
                    table_name = table_name,  
                    where = 'grade', 
                    target = '1')
    if (len(res) == 0) : print('query not found')

    # =============== wjscp ====================
    res = findRecord(select = '*', 
                    table_name = table_name,  
                    where = 'grade', 
                    target = '1')
    if (len(res) == 0) : print('query not found')


    # 4. find columns
    print(f'columns for table"{table_name}" {findColumn(table_name)}')