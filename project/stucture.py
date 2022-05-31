
import os, sys
from bitstring import BitArray
OFFSET = 4


'''
VLR : 최대 길이 100 bytes
'''

class VLR:
    def __init__(self):
        pass


# ============================================================= # 
'''
SLP : 하나의 크기 1000bytes (approximately 10mb)
'''

class SLP:
    def __init__(self):
        pass

# ============================================================= # 

'''
data dictionary 
'''
class dataDic():
    def __init__(self):
        self.tableName = ''
        self.colName = []
        self.colType = []
        self.slotNum = 0
        self.recordNum = 0

    def makeDic(self, table_name:str, col_name: list, col_type:list):
        directory = os.getcwd() + '/table/' + table_name
        # meta data : data dictionay
        # / 로 분할해서 넣을지 아니면 한줄은 이름 한줄은 타입 이렇게 할지
        with open(directory + '/meta_data.txt', 'w+') as f:
            f.write(table_name+'\n')
            # for a,b in zip(col_names, col_type):
            #     f.write(f'{a}/{b} \n')

            for n in col_name:
                f.write(n+ '/')
            f.write('\n')
            for t in col_type:
                f.write(t+ '/')
            f.write('\n0') # total slot 개수
            f.write('\n0') # total record 개수

