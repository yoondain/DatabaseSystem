
import os, sys
from tkinter import OFF
from bitstring import BitArray
from numpy import insert

OFFSET = 4


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

        # 가변 길이 record 시작 위치
        varstart = 1 # 바이트로 바꿔야함/ null byte 앞에 하나 있으므로
        numNeed = [0]
        for coltype, insertcol in zip(col_type, insert_columns):
            if coltype[0] == 'c' :  varstart += int(coltype[1:])
            elif coltype[0] == 'v' : 
                numNeed.append(len(insertcol))
                varstart += OFFSET
        numNeed[0] =varstart
        total_length = sum(numNeed)
        # print(total_length)

        if total_length > VLR_LENGTH : 
            print('out of range')
            return False



        # print(col_type)
        bitmap = bytearray(total_length)
        # 1번째 byte는 null bitmap
        # bitmap[0] = bytearray(0)
        numNeedNum = 1
        bit_start = 1
        print('bitmap')
        for coltype, insertcol in zip(col_type, insert_columns):
            if coltype[0] == 'c' :
                cvarlength = len(insertcol) # 5
                print(insertcol, end = '/')
                # print(len(insertcol.encode()))
                bitmap[bit_start : bit_start + cvarlength] = bytearray(insertcol, encoding='UTF-8')
                # print(bitmap[bit_start : bit_start + varlength])
                bit_start += cvarlength

            elif coltype[0] == 'v':
                start = numNeed[0]
                vvarlength = numNeed[numNeedNum]
                bitmap[bit_start : bit_start + OFFSET//2] =  start.to_bytes(2,'big') # 시작하는 곳
                bitmap[bit_start + OFFSET//2 : bit_start + OFFSET] =  vvarlength.to_bytes(2,'big') # 변수 길이
                # print(bitmap[bit_start:bit_start + OFFSET])


                bitmap[start : start+ vvarlength] = bytearray(insertcol, encoding='UTF-8')





                numNeed[0] += vvarlength
                numNeedNum += 1
                bit_start += OFFSET

            print(coltype)
            















        self.vlr = bitmap
        # print(bitmap[0])
        print(bitmap[1:6])
        print(bitmap[6:10])
        print(bitmap[10:11])
        print(bitmap[11:15])
        print(bitmap[15:19])
        print(bitmap[19:27])
        # print(bitmap)
        # a = bitmap.decode()
        # print(len(bitmap))
        # print(a)

        # print(insert_columns)
        # print(col_type)
        
        










# ============================================================= # 
'''
SLP : 하나의 크기 1000bytes (approximately 10mb)
'''
SLP_LENGH = 1000
class SLP:
    def __init__(self):
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
