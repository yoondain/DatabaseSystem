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

    record = VLR()
    record.makeVLR(tableName, col_name)
    print(f'new insert record :{record.vlr}')
    # record = VLR(tableName, col_name)
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
#   findRecord(select = 'name', tableName = table_name,  where = 'grade', target = '1')
def findRecord(select : str, tableName : str,  where  : str, target : str):
    metadata = dataDict()
    metadata.getDict(tableName)
    result_vlr = []

    # metadata col_name에서 몇번째의 col 값인지 확인해야함
    try:
        col_index = metadata.colName.index(where)
        col_type = metadata.colType[col_index]
    except:
        return result_vlr

    # print(metadata.colName, where)
    print(f'target // col index : {col_index} | col type : {col_type}\n')

    # if metadata.slotNum == 0 : return result_vlr
    # if select not in metadata.colName : return result_vlr
    for slot_num in range(1,metadata.slotNum+1): # 있는 slp 전부 확인
        print(f'{slot_num}th SLP')
        tempslp = SLP()
        tempslp.getSLP(tableName, slot_num) # slot 불러오기
        for i in range(1,tempslp.recordNum + 1): # slot에 있는 record를 하나씩 꺼내기
            '''
            1. record 꺼내기
            '''
            recordStartPoint = int.from_bytes(tempslp.slp[ (2* EACH_HEADER_SIZE) * i : (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE],'big')
            recordLength = int.from_bytes(tempslp.slp[ (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE : 
                                        (2* EACH_HEADER_SIZE) * i + EACH_HEADER_SIZE+ EACH_HEADER_SIZE],'big')
            record = tempslp.slp[recordStartPoint:recordStartPoint+recordLength] # slot_num 번째의 i 번째 record
            print(f'\'{i}\'th record : {record} record length : {len(record)}')


            # ================== class 사용 ============
            # SLPrecord = VLR()
            # SLPrecord.makeVLR_bytearray(record, tableName)          








            '''
            2. null bitmap 확인 ==> 해당 column에 대응하는 값이 0인지 아닌지
            '''
            null_bitmap = record[0:1]
            null_int = int.from_bytes(null_bitmap,'big')
            null_check_str = ('{0:08b}'.format(null_int)) # 길이가 8인 스트링 
            null_check = ('{0:08b}'.format(null_int))[len(null_check_str) - len(metadata.colType) + col_index]
            print(null_check_str)
            print(f'null check : {null_check}')

            if null_check == '1' : 
                #print('null')
                continue # null이면 찾을 필요 없음
            else : # 해당 record가 null이 아닐 때 
                print('not null')


            '''
            3. record 검사
            '''
            # null bitmap도 함께 생각해야함 null이면 계산하면 안됨
            record_pointer = 1 # null부터 시작

            for i in range(0,col_index):
                if null_check_str[-1-i] == '1': continue
                if metadata.colType[i][0] == 'c' and null_check_str[len(null_check_str) - len(metadata.colType) + i] == '0':
                    record_pointer += int(metadata.colType[i][1:])
                elif metadata.colType[i][0] == 'v' and null_check_str[len(null_check_str) - len(metadata.colType) + i] == '0':
                    record_pointer += OFFSET

            if  metadata.colType[col_index][0] == 'c' : 
                # 조건과 비교할 값 check_value
                check_value = record[record_pointer:record_pointer+int(metadata.colType[col_index][1:])]
                # print('---start check---')
                # print(record_pointer)
                # print(target)
                # print(check_value)
                # print(check_value.decode('utf-8'))
                # print(int.from_bytes(check_value,'big'))
                # print(target == check_value.decode('utf-8'))
                # print('---end check---')
                if target == check_value.decode('utf-8') :
                    result_vlr.append(record)


            elif metadata.colType[col_index][0] == 'v':
                # print(type(record[record_pointer : record_pointer + OFFSET//2]))
                stpt = int.from_bytes(record[record_pointer : record_pointer + OFFSET//2],'big')
                length = int.from_bytes(record[record_pointer + OFFSET//2: record_pointer + OFFSET],'big')
                # 조건과 비교할 값 check_value
                check_value = record[stpt:stpt + length]
                print("!!!!!!!!!!val")
                print(target)
                print(check_value.decode('utf-8'))
                print(target == check_value.decode('utf-8'))
                if target == check_value.decode('utf-8') :
                    result_vlr.append(record)



            # if col_type[0] == 'c': # char
            #     for ct in metadata.colType:
            #         if ct[0] == 'c' : record_pointer+=int(ct[1:])
            #         elif ct[0] == 'v': record_pointer +=4

            #     pass

            # elif col_type[0] == 'v': # varchar

            #     pass

            # print(f'record index : {record_pointer}')
                
    print(f'record 개수 : {len(result_vlr)} , {result_vlr}')           
    return result_vlr
            





# =====================================================
def findColumn(tableName: str):
    meta_data = dataDict()
    meta_data.getDict(tableName)
    return meta_data.colName
