import os, sys
from stucture import *
from bitstring import BitArray

def createTable(tableName:str, col_name : list, col_type:list):
    
    # folder 만들 위치
    directory = os.getcwd() + '/table/' + tableName  # /// project.py에서 실행할 때
    # directory = '../../data/' + tableName # /// createTable.py 실행할 때
    print(directory)
    try:
        
        if not os.path.exists(directory):
            print('make')
            os.makedirs(directory)
        else:
            print(f'\"{tableName}\" table already exists')
            # exit()
    except OSError:
        print(r'Error: Creating directory. ' +  directory)
        exit()

    metadata = dataDic()
    metadata.makeDic(tableName,col_name, col_type)
    pass

# =====================================================

def insertColumn(insertQuery:str):
    newRecord = VLR()
    pass

# =====================================================
def findRecord(query : str):
    pass


# =====================================================
def findColumn():
    pass
























if __name__ == '__main__':
    col_names = ['ID','name','grade','dept']
    col_types = ['char(5)','varchar(80)','char(1)','varchar(80)'] 
    tablename = 'student'
    # createTable( tablename,col_names,col_types) # 하나의 테이블당 하나의 folder

    l = 'ID/name/grade/dept/'
    print(l.rstrip().split(sep="/")) # /로 나누기
    # createFolder('../../data/' + tablename)

    '''
    insert into student
    values ('12333' , 'name','3','biology')
    
    '''


    pass







