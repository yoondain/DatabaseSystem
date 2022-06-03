from structure import *

# =====================================================
def createTable(table_name:str, col_name : list, col_type:list):
    metadata = dataDict()
    metadata.makeDict(table_name,col_name, col_type)

# =====================================================
def insertRecord(table_name:str, col_name : list):
    metadata = dataDict()
    metadata.getDict(table_name)
    slotnum = metadata.slotNum
    print(f'now slot num : {slotnum}')
    directory = os.getcwd() + '/table/' + table_name

    record = VLR()
    record.makeVLR(table_name, col_name)
    # print(f'new insert record :{record.vlr}')
    # record = VLR(table_name, col_name)
    # print('lenlen:   '+str(len(record.vlr)))

    updateSlot = False
    updateRecord = False
    
    newSLotNum = 0

    if slotnum == 0 : # 해당 table에 slot이 아예 존재하지 않을 때 -> 새로 1개를 만들어야 한다
        newSLotNum = 1
        # slp 초기화
        newslp = SLP()
        # 개수 update
        newslp.slp[0:EACH_HEADER_SIZE] = (1).to_bytes(EACH_HEADER_SIZE, 'big') # 해당 slp의 record 개수 1개가 됨
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
        newslp = SLP()
        newslp.getSLP(table_name, slotnum) # 가장 마지막에 있는 slot 가져온다
        if newslp.frespaceRemain < len(record.vlr): # 길이 부족하면
            newSLotNum = slotnum + 1
            '''
            ☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆slot 새로 만들어야함 그리고 삽입☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆
            '''
            # slp 초기화 새로 만들어야 하므로!
            newslp = SLP()
            # 개수 update
            newslp.slp[0:EACH_HEADER_SIZE] = (1).to_bytes(EACH_HEADER_SIZE, 'big') # 새로 만든 slp의 record 개수 1개가 됨
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

    metadata.updateDict(table_name, updateSlot, updateRecord ) # meta data update

# =====================================================
def findRecord(select : str, table_name : str,  where  : str, target : str):
    print(where, target)
    metadata = dataDict()
    metadata.getDict(table_name)
    result_vlr = [] # return 할 최종 값

    # metadata col_name에서 몇번째의 col 값인지 확인해야함
    try:
        where_index = metadata.colName.index(where) # dept 의 index
    except:
        return result_vlr

    if select == '*' : select_index = -1 # 전부일때
    else:
        try:  select_index = metadata.colName.index(select) # 'name'의 index
        except:  return result_vlr

    if metadata.slotNum == 0 : return result_vlr # slot 없으면 바로 return
    '''
    if all(select) not in metadata.colName : return result_vlr # 하나라도 없으면 바로 return
    '''

    print(f'target// | {where} = {target}\n')

    

    for slot_num in range(1,metadata.slotNum+1): # 있는 slp 전부 확인
        print(f'{slot_num}th SLP')
        tempslp = SLP()
        tempslp.getSLP(table_name, slot_num) # slot 불러오기
        for i in range(1, tempslp.recordNum + 1): # slot에 있는 record(vlr)를 하나씩 꺼내기
            '''
            1. record 꺼내기
            '''
            recordStartPoint = int.from_bytes(tempslp.slp[ (2* EACH_HEADER_SIZE) * i : 
                                        (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE],'big')
            recordLength = int.from_bytes(tempslp.slp[ (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE : 
                                        (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE+ EACH_HEADER_SIZE],'big')
            record = tempslp.slp[recordStartPoint:recordStartPoint+recordLength] # slot_num 번째의 i 번째 record
            '''
            2. 꺼낸 record bytearray를 VLR로 만들어 정보 가져오기
            '''
            # ================== class 사용 ============
            SLPrecord = VLR()
            SLPrecord.makeVLR_bytearray(record, table_name) # 1에 해당하는 record를 만드는 것  
            #SLPrecord.printVLR()
        
            # print(SLPrecord.value[where_index])
            # print(SLPrecord.isNotNull)
            '''
            3. target 값 비교
            '''
            if SLPrecord.isNotNull[where_index] != False and SLPrecord.value[where_index] == target:
                if select_index == -1: result_vlr.append(SLPrecord.value)
                else: result_vlr.append(SLPrecord.value[select_index])
                    
                
    print(f'record 개수 : {len(result_vlr)} , {result_vlr}')           
    return result_vlr
            
# =====================================================
def findColumn(table_name: str):
    meta_data = dataDict()
    meta_data.getDict(table_name)
    return meta_data.colName
