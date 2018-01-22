from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import *
from sqlalchemy import event


from datetime import *
import pandas as pd
import numpy as np
import sys


# 定义引擎，连接数据库
'''
注：
 create_engine方法参数('使用数据库+数据库链接模块://数据库用户名:密码@ip地址:端口/要连接的数据库名称',echo=True表示是否查看生成的sql语句)
 create_engine方法进行数据库连接，返回一个 db 对象
 engine.execute("SELECT * FROM user") 叫做connnectionless执行，不预先连接
 engine.connect()获取conn, 然后通过conn.execute(), 叫做connection执行
 官方推介后者
'''
# engine = create_engine('mysql+pymysql://root:liu123@localhost:3306/bstest_schema?charset=utf8',convert_unicode=True,echo=True)
engine = create_engine('oracle://test2:test2@10.21.68.211:1521/hsfa?charset=utf8',convert_unicode=True,echo=False)

#生成ORM基类
Base = declarative_base()

#绑定元信息( 所有 table 对象的集合描述 )
metadata = MetaData(engine)

#获取数据库链接，以备后面使用！！！！！
# conn = engine.connect()
Session = sessionmaker(bind=engine)
session_obj = Session() # class session -> object
# server_default=id_seq.next_value()
id_seq = Sequence('id_seq', start=0, increment=1, minvalue=0, maxvalue=10000, metadata=metadata)

new_table = Table(
    'new_table',
    metadata,
    # Column('id', Integer, id_seq, server_default=id_seq.next_value(), primary_key=True),
    Column('id', Integer, Sequence('id_seq'), primary_key=True),
    Column('time',DateTime,default=datetime.now()),
    Column('time1',DateTime,default=datetime.now()),
    Column('product_code', String(32)),
    Column('acc_id', Integer()),
    Column('product_name', String(50)),
    Column('product_share', Integer()),
    Column('manage_asset_value', Numeric(20, 2)),
    Column('current_nav', Numeric(12, 2)),
    Column('warning_line', Numeric(12, 2)),
    Column('liquidation_line', Numeric(12, 2)),
    Column('remarks', String(50)),
    Column('report_date', String(50)),
    Column('last_modified_date', DateTime, default=datetime.now),
    # Column('created_on', DateTime(), default=datetime.now),
    Column('time', DateTime, default=datetime.now()),
    Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)
)

metadata.create_all(engine) # 会找到 BaseModel 的所有子类，并在数据库中建立这些表

#-------------------------------------------------------------------------------------------
## mapper
new_table = Table('new_table',metadata,autoload = True)
class newtable_class(object):
    pass
mapper(newtable_class,new_table)# 映射orm
c = new_table.c
print(c)
#---------------------------------------------------------------------------------------------
## mapper 事件监听
def handler_before(mapper, connection, target):
    target.time = datetime(2018,1,2,15,3,00)
    pass

# event.listen(newtable_class,'before_insert',handler_before)
# event.listen(newtable_class,'before_update',handler_before)

def handler_after(mapper,connection,target):
    #在这里操作要用connection
    target.time = datetime(2018,1,2,15,9,00)
    connection.execute(
        new_table.update().
            where(new_table.c.id==target.id).
            values(time = target.time)
    )
    pass

# event.listen(newtable_class,'after_insert',handler_after)
# event.listen(newtable_class,'after_update',handler_after)

# # mapper  add
newtable_obj = newtable_class()# 构建一个对象
# newtable_obj.id = 32
# newtable_obj.time = datetime.now()


# newtable_obj_1 = newtable_class()
newtable_obj_2 = newtable_class()


# session_obj.merge( newtable_obj )#有就更新，没有就插入
# session_obj.merge( newtable_obj_1)
session_obj.merge( newtable_obj_2)

session_obj.commit()


# {'product_code': 'SH1385', 'acc_id': 900166022, 'product_name': '中信期货金富招商1号', 'product_share': 14000,
# 'manage_asset_value': 5, 'current_nav': 0, 'warning_line': 0.9, 'liquidation_line': 0.88,
# 'remarks': '持仓股票停牌，二次清算', 'report_date': 20171207}]


pass


