from matplotlib.pyplot import table
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

    if slotnum == 0 : # slot 아무것도 존재하지 않을 때
        newSLotNum = 1
        # 개수 update
        newslp.slp[0:EACH_HEADER_SIZE] = (1).to_bytes(EACH_HEADER_SIZE, 'big') # record 개수 1개가 됨
        # start of freespace UPDATE
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

    else: # 하나라도 있을 때
        newslp.getSLP(tableName, slotnum) # 가장 마지막에 있는 slot 가져온다
        if newslp.frespaceRemain < len(record.vlr):
            newSLotNum = slotnum + 1
            '''
            ☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆slot 새로 만들어야함 그리고 삽입☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆
            '''
            pass
        else:
            newSLotNum = slotnum 
            # 개수 update done
            newslp.slp[0:EACH_HEADER_SIZE] = (newslp.recordNum+1).to_bytes(EACH_HEADER_SIZE, 'big') # record 개수 + 1 해서 저장
            # start of freespace UPDATE done
            newslp.slp[EACH_HEADER_SIZE: EACH_HEADER_SIZE*2] = ( 2 * EACH_HEADER_SIZE * (newslp.recordNum + 2 ) ).to_bytes(EACH_HEADER_SIZE,'big') # free space 시작



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

    metadata.updateDict(tableName, updateSlot, updateRecord ) # meta data update

# =====================================================
def findRecord(select : str, tableName : str,  where : str):
    metadata = dataDict()
    metadata.getDict(tableName)
    result_vlr = []
    if metadata.slotNum == 0 : return result_vlr
    if select not in metadata.colName : return result_vlr
    
    for slot_num in range(1,metadata.slotNum+1):
        tempslp = SLP()
        tempslp.getSLP(tableName, slot_num) # slot 불러오기
        for i in range(1,tempslp.recordNum+1):
            recordStartPoint = int.from_bytes(tempslp.slp[ (2* EACH_HEADER_SIZE) * i : (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE],'big')
            recordLength = int.from_bytes(tempslp.slp[ (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE: (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE+ EACH_HEADER_SIZE],'big')
            record = tempslp.slp[recordStartPoint:recordStartPoint+recordLength]
            print(f'\'{i}\'th record : {record}')



        pass



# =====================================================
def findColumn(tableName: str):
    meta_data = dataDict()
    meta_data.getDict(tableName)
    return meta_data.colName
