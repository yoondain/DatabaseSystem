
import os, sys

from numpy import byte


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
    def __init__(self):
        self.vlr = bytearray(VLR_LENGTH)
        self.tableName = ''
        self.nullbitmap = ''
        self.isNull=[]

        self.colType = [] #
        self.colName = []
        self.value = []

        print('make new VLR')
        

    def makeVLR(self, tableName, insert_columns): # table이랑 insert colum이랑 
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
        varstart = 1 
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


        ### null check 해야함 --> return 된 tf bool 리스트가지고
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
                # print(vvarlength)
                bitmap[bit_start : bit_start + OFFSET//2] =  start.to_bytes(2,'big') # 시작하는 곳
                bitmap[bit_start + OFFSET//2 : bit_start + OFFSET] =  vvarlength.to_bytes(2,'big') # 변수 길이
                # print(bitmap[bit_start:bit_start + OFFSET])

                bitmap[start : start+ vvarlength] = bytearray(insertcol, encoding='UTF-8') # 뒤쪽에 정보 저장

                numNeed[0] += vvarlength
                numNeedNum += 1
                bit_start += OFFSET
            # print(bitmap)
            # print(coltype)
            




        
        # print(bitmap[0])
        print(f'final bitmap: {bitmap} bitmap length : {len(bitmap)}')
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

    def makeVLR_bytearray(self, bytes, tableName):
        metadata = dataDict()
        metadata.getDict(tableName)
        
        self.colType = metadata.colType
        self.colName = metadata.colName

        null_bitmap = bytes[0:1]
        null_int = int.from_bytes(null_bitmap,'big')
        null_check_str = ('{0:08b}'.format(null_int)) # 길이가 8인 스트링 
        null_check = ('{0:08b}'.format(null_int))[len(null_check_str) - len(metadata.colType) + col_index]
        print(null_check_str)
        print(f'null check : {null_check}')





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

    def getVLR(self): # VLR의 정보 가져오는 것
        
        pass
    def printVLR(self): # VLR의 정보 prints
        print(f'bytearray : {self.vlr}')
        print(f'bytearray length : {len(self.vlr)}')

        print(f'null bit map : {self.nullbitmap}')
        print(f'coltype : {self.colType}')
        print(f'coltype : {self.isNull}')
        print(f'coltype : {self.colType}')
        print(f'coltype : {self.value}')
        


# ============================================================= # 
'''
SLP : 하나의 크기 1000bytes 
'''
SLP_LENGTH = 100
END_OF_SLP = 100

EACH_HEADER_SIZE = 3
# ENTRY_SIZE = 5
# PAGE_SIZE = 1000
# ----

class SLP:
    def __init__(self):
        bytemap = bytearray(SLP_LENGTH)
        # print(f'bytemap : {bytemap[:10]}, length : {len(bytemap)}')
        bytemap[0:EACH_HEADER_SIZE] = (0).to_bytes(EACH_HEADER_SIZE, 'big') # record 개수는 0으로 초기화
        bytemap[EACH_HEADER_SIZE: EACH_HEADER_SIZE*2] = (10).to_bytes(EACH_HEADER_SIZE,'big') # free  space의 start
        
        self.slp = bytemap
        self.recordNum = 0
        self.freespaceStart = 10 # 이게 10이라는 건 아무것도 없다는 것
        self.freespaceEnd = SLP_LENGTH 
        self.frespaceRemain = self.freespaceEnd - self.freespaceStart
 
    def getSLP(self, tableName : str, slotNum : int):
        directory = os.getcwd() + '/table/' + tableName
        # tempslp = SLP()#bytearray(EACH_HEADER_SIZE)
        with open(directory + '/slot'+str(slotNum)+'.bin', 'rb') as f:
            self.slp = bytearray(f.read())
            #print(self.slp)
        self.recordNum = int.from_bytes(self.slp[0:EACH_HEADER_SIZE],'big')
        self.freespaceStart = int.from_bytes(self.slp[EACH_HEADER_SIZE:2*EACH_HEADER_SIZE],'big')
        self.freespaceEnd = int.from_bytes(self.slp[2*EACH_HEADER_SIZE * self.recordNum :2*EACH_HEADER_SIZE * self.recordNum + EACH_HEADER_SIZE],'big')
        self.frespaceRemain = self.freespaceEnd - self.freespaceStart

    def printSLP(self):
        print(f'record num : {self.recordNum}')
        print(f'freespace start : {self.freespaceStart}')
        print(f'freespace end : {self.freespaceEnd}')
        print(f'freespace remain : {self.frespaceRemain}')


        
        

    # def __init__(self,tableName: str, record : VLR):
    #     self.slp = bytearray(SLP_LENGTH)
        



    #     pass

# ============================================================= # 

'''
data dictionary 
'''
class dataDict:
    def __init__(self):
        self.tableName = ''
        self.colName = []
        self.colType = []
        self.slotNum = 0
        self.recordNum = 0

    def makeDict(self, table_name:str, col_name: list, col_type:list):
        directory = os.getcwd() + '/table/' + table_name
        try:
            if not os.path.exists(directory):
                # print('make')
                os.makedirs(directory)
            else:
                pass
                #print(f'\"{tableName}\" table already exists')
                # exit()
        except OSError:
            print(r'Error: Creating directory. ' +  directory)
            exit()

        
        # meta data write
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
            self.slotNum = int(f.readline().rstrip())
            self.recordNum = int(f.readline().rstrip())

    def updateDict(self, tableName : str, updateSlot : bool, updateRecord:bool):
        metadata = dataDict()
        metadata.getDict(tableName)
        directory = os.getcwd() + '/table/' + tableName
        with open(directory + '/meta_data.txt', 'w+') as f:
            f.write(tableName+'\n')
            for i,n in enumerate(metadata.colName):
                f.write(n)
                if i != len(metadata.colName)-1: f.write('/')
            f.write('\n')
            for q,t in enumerate(metadata.colType):
                f.write(t)
                if q != len(metadata.colType)-1: f.write('/')
            if updateSlot : f.write('\n'+str(metadata.slotNum+1))
            else:  f.write('\n'+str(metadata.slotNum))
            if updateRecord : f.write('\n'+str(metadata.recordNum+1))
            else:  f.write('\n'+str(metadata.recordNum))
