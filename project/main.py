import os, sys
from API import *
from stucture import *


if __name__ == '__main__':
    
    col_names = ['ID','name','grade','dept']
    col_types = ['char(5)','varchar(80)','char(1)','varchar(100)'] 
    
    createTable('dd',col_names,col_types )    
    # print(sys.argv)
    
    pass
