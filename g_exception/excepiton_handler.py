import sys
import datetime
import logging
import os
from PIL import ImageGrab
import time





# ----------------------------------------------- ：错误日志
'''
（日志级别等级CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET），
 默认的日志格式为——日志级别：Logger名称：用户输出消息。
'''

## 要在任何打印log之前设置才有效
def set_logging():
    print('log workspace: ',os.getcwd())
    local_path = os.path.realpath(sys._getframe().f_code.co_filename)  # 本文件
    local_dir = os.path.split(local_path)[0]  # 本文件目录

    logging.basicConfig(level=logging.INFO,
                        # format='%(asctime)s [%(filename)s] [line:%(lineno)d] %(levelname)s :\n    %(message)s',# 调用logging 输出的行，并不是出错行
                        format='-' * 100 + '%(asctime)s %(levelname)s :\n'
                               '    %(message)s',
                        datefmt='%a, %d %b %Y, %H:%M:%S',
                        filename=os.path.join( local_dir,'error.log'),
                        filemode='a')


## 错误日志
def error_log( exctype, value, traceback ):
    logging.error('uncaught exception',exc_info=(exctype, value, traceback)) # exc_infoc为元组 会自动去调用栈和错误信息


# ----------------------------------------------- ：截图
## 错误截图
def screen_shot():
    time.sleep(2)
    local_path = os.path.realpath(sys._getframe().f_code.co_filename)  # 本文件
    local_dir = os.path.split(local_path)[0]  # 本文件目录
    img_dir = os.path.join(local_dir, './error_img')
    print('error_img dir:   ',img_dir)
    current_time = datetime.datetime.now().strftime('%d-%b-%Y_%H-%M-%S')
    path = os.path.join( img_dir, current_time + '.png')
    im = ImageGrab.grab()
    im.save(path)  # 定义保存的路径和保存的图片格式



## 异常处理
def exceptHandler( exctype, value, tb ):
    sys.__excepthook__( exctype, value, tb ) ## 默认处理函数,输出信息
    error_log( exctype, value, tb)
    screen_shot( )



#设置异常处理函数
def g_except_config( ):
    set_logging()
    sys.excepthook = exceptHandler


# ----------------------------------------------- ：test
def error_test():
    g_except_config()
    x = 1/0


if __name__ == '__main__':

    error_test()
    os.system("pause")

