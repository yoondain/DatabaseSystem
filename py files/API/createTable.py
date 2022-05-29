import os, sys

from matplotlib.pyplot import table

# def createFoler(directory):
#     print(directory)
#     try:
#         if not os.path.exists(directory):
#             print('make')
#             os.makedirs(directory)
#         else:
#             print(1)
#     except OSError:
#         print(r'Error: Creating directory. ' +  directory)
                


def createTable(tableName:str, col_names : list, col_type:list):
    
    # folder 만들 위치
    directory = os.getcwd() + '/data/' + tableName  # /// project.py에서 실행할 때
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

    # meta data : data dictionay
    # / 로 분할해서 넣을지 아니면 한줄은 이름 한줄은 타입 이렇게 할지
    with open(directory + '/meta_data.txt', 'w+') as f:
        f.write(tableName+'\n')
        for a,b in zip(col_names, col_type):
            f.write(f'{a}/{b} \n')

        # for n in col_names:
        #     f.write(n+'\n')
        # for t in col_type:
        #     f.write(t+'\n')
    pass



if __name__ == '__main__':

    tablename = 'student'
    createTable( tablename,[],[]) # 하나의 테이블당 하나의 folder

    # createFolder('../../data/' + tablename)
    pass