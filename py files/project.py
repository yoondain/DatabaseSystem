from distutils.command.build_scripts import first_line_re
import os, sys
from API.createTable import *
from API.findColumn import *
from API.findRecord import *
from API.insertColumn import *


if __name__ == '__main__':
    
    col_names = ['ID','name','grade','dept']
    col_types = ['char(5)','varchar(80)','char(1)','varchar(80)'] # f5 : fixed length 5 / v80 : variable length 80
    
    
    createTable('student1',col_names,col_types )
    
    # findRecord('select name from student where name = \'dain\'')
    
    print(sys.argv)
    
    pass
