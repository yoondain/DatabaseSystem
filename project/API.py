from turtle import update
from structure import *

# =====================================================
def createTable(tableName:str, col_name : list, col_type:list):
    metadata = dataDict()
    metadata.makeDict(tableName,col_name, col_type)

# =====================================================
def insertColumn(tableName:str, col_name : list):
    metadata = dataDict()
    metadata.getDict(tableName)
    slotnum = metadata.slotNum
    directory = os.getcwd() + '/table/' + tableName


    record = VLR(tableName, col_name)

    updateSlot = False
    updateRecord = False
    newslp = SLP()
    if slotnum == 0 :
        
        newslp.freespaceEnd -= len(record.vlr)

        # 개수 update
        newslp.slp[0:EACH_HEADER_SIZE] = (1).to_bytes(EACH_HEADER_SIZE, 'big') # record 개수 1개가 됨
        # start of freespace UODATE
        newslp.slp[EACH_HEADER_SIZE: EACH_HEADER_SIZE*2] = (EACH_HEADER_SIZE * 4).to_bytes(EACH_HEADER_SIZE,'big') # free space 시작
        
        # 새로운 record 삽입
        newslp.slp[-len(record.vlr):] = record.vlr

        # 삽입한 record의 offset - 시작위치 알려주는
        newslp.slp[EACH_HEADER_SIZE*2 : EACH_HEADER_SIZE*3] = (newslp.freespaceEnd).to_bytes(EACH_HEADER_SIZE,'big')
        # 삽입한 record의 offset - length 알려줌
        newslp.slp[EACH_HEADER_SIZE*3 : EACH_HEADER_SIZE*4] = (len(record.vlr)).to_bytes(EACH_HEADER_SIZE,'big')
        
        #newslp.freespaceStart +=EACH_HEADER_SIZE*2
        


        updateSlot = True
        updateRecord = True
        
        with open(directory + '/slot1.bin', 'wb') as f:
            f.write(newslp.slp)

        

    else:
        pass

    metadata.updateDict(tableName, updateSlot, updateRecord )




    # 
    # newRecord = VLR(tableName,col_name)
    # newslp.insertRec(tableName, newRecord.vlr)


# =====================================================
def findRecord(tableName:str, query : str):
    metadata = dataDict()
    metadata.getDict(tableName)


# =====================================================
def findColumn(tableName: str):
    meta_data = dataDict()
    meta_data.getDict(tableName)
    return meta_data.colName
