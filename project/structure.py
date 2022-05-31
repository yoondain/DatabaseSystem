
import os, sys
from this import s
from tkinter import OFF
from bitstring import BitArray
from matplotlib.colors import to_rgb
from numpy import insert

OFFSET = 4


def mapping(x):
    # null 이 아니면 0
    if x == '0' : return True
    else: return False

# ============================================================= # 
'''
VLR : 최대 길이 40 bytes
'''
VLR_LENGTH = 40

class VLR:
    def __init__(self, tableName, insert_columns):
        meta_data = dataDict()
        meta_data.getDict(tableName)
        col_type = meta_data.colType # c인지 v인지

        '''
        # =========== null bitmap 에 들어갈 숫자 int 형으로 / null인지 아닌지 bool type의 list
        '''
        null_bitmap, tf = self.checkNull(insert_columns)
        # print(tf)

        '''
        # =========== 필요한 숫자 추출 ex) offset 시작 위치 //null값 
        '''
        varstart = 1 # 바이트로 바꿔야함/ null byte 앞에 하나 있으므로
        numNeed = [0]
        for coltype, insertcol, ttff in zip(col_type, insert_columns,tf):
            if ttff == False: continue # null이면 pass
            if coltype[0] == 'c' :  varstart += int(coltype[1:])
            elif coltype[0] == 'v' : 
                numNeed.append(len(insertcol))
                varstart += OFFSET
        numNeed[0] =varstart
        total_length = sum(numNeed)
        # print(total_length)

        '''
        # =========== self init 할 byte를 totel_length 만큼 할당 
        '''
        bitmap = bytearray(total_length)
        bitmap[0:1] = null_bitmap.to_bytes(1,'big')
        
        '''
        # =========== null bitmap 제외한 [1:] 정보 저장
        '''
        numNeedNum = 1
        bit_start = 1
        # print('bitmap')


        ### null check 해야함 --> 57번째의 tf 가지고
        # print(f'bitmap length : {len(bitmap)}' )
        for coltype, insertcol, ttff in zip(col_type, insert_columns , tf):

            if ttff == False: continue # null 이면 아무것도 하지 말기
                
            if coltype[0] == 'c' :
                cvarlength = len(insertcol)
                bitmap[bit_start : bit_start + cvarlength] = bytearray(insertcol, encoding='UTF-8')
                bit_start += cvarlength

            elif coltype[0] == 'v':
                start = numNeed[0]
                vvarlength = numNeed[numNeedNum]
                print(vvarlength)
                bitmap[bit_start : bit_start + OFFSET//2] =  start.to_bytes(2,'big') # 시작하는 곳
                bitmap[bit_start + OFFSET//2 : bit_start + OFFSET] =  vvarlength.to_bytes(2,'big') # 변수 길이
                # print(bitmap[bit_start:bit_start + OFFSET])

                bitmap[start : start+ vvarlength] = bytearray(insertcol, encoding='UTF-8') # 뒤쪽에 정보 저장

                numNeed[0] += vvarlength
                numNeedNum += 1
                bit_start += OFFSET
            print(bitmap)
            # print(coltype)
            




        
        # print(bitmap[0])
        print(f'final bitmap: {bitmap}')
        # print(bitmap[0:1]) # null

        # print(bitmap[1:6].decode()) # id
        # print(bitmap[6:10]) # offset - name
        # print(bitmap[10:11].decode()) # grade
        # print(bitmap[11:15]) # offset - dept

        # print(bitmap[15:19].decode()) # name
        # print(bitmap[19:].decode()) # dept
        # print(bitmap)
        # a = bitmap.decode()
        # print(len(bitmap))
        # print(a)

        # print(insert_columns)
        # print(col_type)



        self.vlr = bitmap # 이 bitmap으로 초기화


    # ============================================       
    def checkNull(self, insert_columns):
        null_bitmap_string = ['0','0','0','0','0','0','0','0']
        for i,ins in enumerate(reversed(insert_columns)):
            if ins == 'null': null_bitmap_string[7-i] = '1'
            else: null_bitmap_string[7-i] = '0'

        bitmap_number = int("".join(null_bitmap_string ),2)
    
        # byte바꾸기 전 정수, null 값인지 [True, True, False, False] 두개 return 
        tf = null_bitmap_string[-len(insert_columns):]
        ptf = list(map(mapping, tf))

        return bitmap_number , ptf






# ============================================================= # 
'''
SLP : 하나의 크기 1000bytes (approximately 10mb)
'''
SLP_LENGH = 1000
class SLP:
    def __init__(self,tableName: str, record : VLR):
        pass

# ============================================================= # 

'''
data dictionary 
'''
class dataDict():
    def __init__(self):
        self.tableName = ''
        self.colName = []
        self.colType = []
        self.slotNum = 0
        self.recordNum = 0

    def makeDict(self, table_name:str, col_name: list, col_type:list):
        directory = os.getcwd() + '/table/' + table_name

        with open(directory + '/meta_data.txt', 'w+') as f:
            f.write(table_name+'\n')
            for i,n in enumerate(col_name):
                f.write(n)
                if i != len(col_name)-1: f.write('/')
            f.write('\n')
            for q,t in enumerate(col_type):
                f.write(t)
                if q != len(col_type)-1: f.write('/')
            f.write('\n0') # total slot 개수
            f.write('\n0') # total record 개수

    def printDict(self):
        print(f'table name : {self.tableName}')
        print(f'columns : {self.colName}')
        print(f'columns type : {self.colType}')
        print(f'# of slots : {self.slotNum}')
        print(f'# of records : {self.recordNum}')

    def getDict(self, table_name):
        directory = os.getcwd() + '/table/' + table_name
        with open(directory + '/meta_data.txt', 'r') as f:
            self.tableName = f.readline().rstrip()
            self.colName = f.readline().rstrip().split(sep="/")
            self.colType = f.readline().rstrip().split(sep="/")
            self.slotNum = f.readline().rstrip()
            self.slotNum = f.readline().rstrip()



'''
def checkNull(insert_columns):
    null_bitmap_string = ['0','0','0','0','0','0','0','0']
    for i,ins in enumerate(reversed(insert_columns)):
        if ins == 'null': null_bitmap_string[7-i] = '1'
        else: null_bitmap_string[7-i] = '0'

    bitmap_number = int("".join(null_bitmap_string ),2)

    # byte로 바꾼거랑, [True, True, False, False] 두개 return 
    print(null_bitmap_string)
    tf = null_bitmap_string[-len(insert_columns):]
    print(tf)
    ptf = list(map(mapping, tf))
    print(bitmap_number,ptf)
'''

