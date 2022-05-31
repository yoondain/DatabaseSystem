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
    print(f'now slot num : {slotnum}')
    directory = os.getcwd() + '/table/' + tableName


    record = VLR(tableName, col_name)
    # print('lenlen:   '+str(len(record.vlr)))

    updateSlot = False
    updateRecord = False
    newslp = SLP()
    newSLotNum = 0

    if slotnum == 0 :
        newSLotNum = 1
        # 개수 update
        newslp.slp[0:EACH_HEADER_SIZE] = (1).to_bytes(EACH_HEADER_SIZE, 'big') # record 개수 1개가 됨
        # start of freespace UODATE
        newslp.slp[EACH_HEADER_SIZE: EACH_HEADER_SIZE*2] = (EACH_HEADER_SIZE * 4).to_bytes(EACH_HEADER_SIZE,'big') # free space 시작
        # 새로운 record 삽입
        newslp.slp[-len(record.vlr):] = record.vlr # newslp.slp[newslp.freespaceEnd-len(record.vlr):newslp.freespaceEnd] = record.vlr
        # 삽입한 record의 offset - 시작위치 알려주는
        newslp.freespaceEnd -= len(record.vlr)
        newslp.slp[EACH_HEADER_SIZE*2 : EACH_HEADER_SIZE*3] = (newslp.freespaceEnd).to_bytes(EACH_HEADER_SIZE,'big')
        # 삽입한 record의 offset - length 알려줌
        newslp.slp[EACH_HEADER_SIZE*3 : EACH_HEADER_SIZE*4] = (len(record.vlr)).to_bytes(EACH_HEADER_SIZE,'big')
        updateSlot = True
        updateRecord = True
        

            

    else:
        newslp.getSLP(tableName, slotnum) # 가장 마지막에 있는 slot 가져온다
        if newslp.frespaceRemain < len(record.vlr):
            newSLotNum = slotnum + 1
            #새로 만들어야함
            pass
        else:
            newSLotNum = slotnum 
            # 개수 update done
            newslp.slp[0:EACH_HEADER_SIZE] = (newslp.recordNum+1).to_bytes(EACH_HEADER_SIZE, 'big') # record 개수 + 1 해서 저장
            # start of freespace UPDATE done
            newslp.slp[EACH_HEADER_SIZE: EACH_HEADER_SIZE*2] = (2 * EACH_HEADER_SIZE * (newslp.recordNum + 1 ) ).to_bytes(EACH_HEADER_SIZE,'big') # free space 시작



            # 새로운 record 삽입 done
            newslp.slp[newslp.freespaceEnd-len(record.vlr):newslp.freespaceEnd] = record.vlr

            # 삽입한 record의 offset - 시작위치 알려주는
            newslp.freespaceEnd -= len(record.vlr)
            newslp.slp[EACH_HEADER_SIZE*2 * (newslp.recordNum+1) : EACH_HEADER_SIZE*2 * (newslp.recordNum+1) + EACH_HEADER_SIZE] = (newslp.freespaceEnd).to_bytes(EACH_HEADER_SIZE,'big')
            # 삽입한 record의 offset - length 알려줌
            newslp.slp[EACH_HEADER_SIZE*2 * (newslp.recordNum+1) + EACH_HEADER_SIZE : EACH_HEADER_SIZE*2 * (newslp.recordNum+1) + EACH_HEADER_SIZE *2] = (len(record.vlr)).to_bytes(EACH_HEADER_SIZE,'big')

            updateSlot = False
            updateRecord = True
            



        pass
   

    print(f'newly updated slp : {newslp.slp}')
    with open(directory + '/slot' + str(newSLotNum)+ '.bin', 'wb+') as f:
        f.write(newslp.slp)

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
