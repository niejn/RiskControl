
# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  @file     models/base.py
#  @author   kaka_ace <xiang.ace@gmail.com>
#  @date
#  @brief
#
from sqlalchemy import ForeignKey, Sequence, Boolean

from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import CHAR, Integer, String
from sqlalchemy.ext.declarative import declarative_base
# from datetime import *
from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, DateTime
from sqlalchemy import ForeignKey, Sequence

from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import CHAR, Integer, String
from sqlalchemy.ext.declarative import declarative_base
# from datetime import *
from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import (
    declarative_base,
    DeclarativeMeta,
)

from sqlalchemy.orm import aliased

import datetime
import os

import math
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, Column, Integer, Sequence
# metadata = MetaData()
# engine = create_engine('sqlite:///trades.sqlite')
# trades = Table('trades', metadata, autoload=True, autoload_with=engine)
#
# ins = trades.insert()
# # result = connection.execute(ins, inventory_list)
# connection = engine.connect()
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper


def readAll(path, fileType):
    files = os.listdir (path)
    # textContainer = []
    excelFileList = []
    for file in files:
        file = path + '/' + file
        if not os.path.isfile (file):
            continue
        if file.endswith (fileType):
            excelFileList.append (file)
            print (file)

    return excelFileList


# 日期格式: 2017-12-13
def get_time_futureid_v1(fn=None):
    # fn = '交易统计信息_2017-12-11-5年期国债.xml'
    ans = fn.split ('-', 1)
    pre_ans = ans[0]
    tail_ans = ans[1]

    ftime = tail_ans.split ('.')[0]
    ftime = ftime.replace ('-', '')

    return ftime


def get_time_futureid(fn=None):
    # fn = '交易统计信息_2017-12-11-5年期国债.xml'
    ans = fn.rsplit ('-', 1)
    pre_ans = ans[0]
    tail_ans = ans[1]
    pre_split = pre_ans.split ('_')
    tail_split = tail_ans.split ('.')
    ftime = pre_split[1]
    ftime = ftime.replace ('-', '')
    future_id = tail_split[0]
    return (ftime, future_id)

def get_time(fn=None):
    # fn = '日期：2017-12-7'
    ans = fn.rsplit ('：', 1)
    tail_ans = ans[1]
    time = datetime.datetime.strptime (tail_ans, '%Y-%m-%d')
    str = time.strftime ('%Y%m%d')
    int_time = int(str)

    return int_time, time


def get_data(futures_path='xls/futures', file_type='xlsx'):
    fut_files = readAll (futures_path, file_type)
    df_list = []
    for a_file in fut_files:
        # df = pd.read_csv(a_file, header=0, skipfooter=1, encoding='python', encoding='utf8')
        # pandas的索引函数主要有三种：
        # loc 标签索引，行和列的名称
        # iloc 整型索引（绝对位置索引），绝对意义上的几行几列，起始索引为0
        # ix 是 iloc 和 loc的合体
        # at是loc的快捷方式
        # iat是iloc的快捷方式

        # df = pd.read_excel(a_file, header=2, skip_blank_lines=True)
        df = pd.read_excel (a_file, header=None, skip_blank_lines=True)
        print (df.iloc[0])
        print (df.iloc[1])
        print (df.iloc[2])

        time_row = df.iloc[1]
        time_row = time_row.dropna().tolist()
        time_str = None
        for element in time_row:
            if '日期' in element:
                time_str = element
        if not time_row:
            return
        int_time,file_time = get_time(time_str)

        clean_df = df[3:]
        clean_df = clean_df.reset_index(drop=True)
        print (clean_df)
        print(df.iloc[2])
        print (df.iloc[2].tolist ())
        clean_df.columns = df.iloc[2].tolist()
        clean_df.rename(columns=lambda x: x.strip(), inplace=True)
        number_cols = { '投资者代码', '产品份额', '最新净值', '单位净值', '预警线', '清盘线'}
        old_col_names = clean_df.columns.tolist()
        for col in old_col_names:
            if col in number_cols:
                clean_df[col] = clean_df[col].replace(['-', '不设清盘线'], [0, 0])
                # clean_df[col].replace ({'-':0, '不设清盘线':0})
                clean_df[col] = clean_df[col].fillna (0)
            else:
                clean_df[col] = clean_df[col].replace({'-': '0', '不设清盘线': '0'})
                clean_df[col] = clean_df[col].fillna('0')
        # clean_df = clean_df.replace ('-', 0)
        # clean_df = clean_df.fillna (0)
        print (clean_df)

        # 备案号product_code	 投资者代码acc_id	产品名称product_name	产品份额product_share
        # 最新净值manage_asset_value
        # 单位净值current_nav
        # 预警线warning_line	清盘线 Liquidation line	备注remarks

        cols_dict = {'备案号': 'product_code', '投资者代码': 'acc_id', '产品名称': 'product_name',
                     '产品份额': 'product_share', '最新净值': 'manage_asset_value',
                     '单位净值': 'current_nav',
                     '预警线': 'warning_line', '清盘线': 'liquidation_line', '备注': 'remarks'}
        clean_df.rename (columns=lambda x: x.strip (), inplace=True)
        clean_df.rename (columns=cols_dict, inplace=True)
        clean_df['report_date'] = int_time
        # clean_df['id'] = 0
        print(clean_df)
        df_list.append (clean_df)
    return df_list


def insert_db(data, tablename='trades'):
    metadata = MetaData ()
    # engine = create_engine ('sqlite:///trades.sqlite')
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    # engine = create_engine ('sqlite:///test1.sqlite')
    # engine = create_engine ('sqlite:///test1.sqlite')
    source_constr = hsfa_constr
    engine = create_engine (source_constr, encoding='gbk', echo=True)

    connection = engine.connect ()
    # transaction = connection.begin()

    try:

        data.to_sql (tablename.lower (), engine, if_exists='append', index=False)

        # transaction.commit()
        ans = True
    except IntegrityError as error:
        # transaction.rollback()
        print (error)
    except Exception as error:
        # transaction.rollback()
        print (error)
    finally:
        connection.close ()
    return


def insert_direct(data_list=None, tablename='trades'):
    # metadata = MetaData()
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    # engine = create_engine ('sqlite:///test1.sqlite')
    # engine = create_engine ('sqlite:///test1.sqlite')
    # 生成ORM基类
    Base = declarative_base()
    source_constr = hsfa_constr
    engine = create_engine (source_constr, encoding='gbk', echo=True)
    connection = engine.connect ()
    # meta = MetaData(bind=engine, reflect=True)
    # o_table = meta.tables[tablename.lower()]
    meta = MetaData (bind=engine)
    meta = MetaData(bind=engine, reflect=True)
    from sqlalchemy import DateTime
    o_table = Table (tablename.lower (), meta,
                     Column('id', Integer, Sequence('id_seq'), primary_key=True),
                     Column('last_modified_date', DateTime, default=datetime.datetime.now),
                     autoload=True)
    # t = Table('mytable', metadata,
    #           Column('id', Integer, Sequence('id_seq'), primary_key=True),
    #           autoload=True
    #           )
    Session = sessionmaker(bind=engine)
    session_obj = Session()

    class newtable_class(object):
        pass

    mapper(newtable_class, o_table)


    # action = o_table.insert().values(data_list[0])

    ans = False
    try:
        print(data_list)
        # newtable_obj_1 = newtable_class()
        # newtable_obj_1.time = datetime(2018,1,2,15,1,00)

        # newtable_obj_2 = newtable_class(data_list)
        # newtable_obj_2.time = datetime(2018,1,2,15,2,00)

        # session_obj.merge( newtable_obj )#有就更新，没有就插入
        # session_obj.merge(newtable_obj_1)
        # session_obj.merge(newtable_obj_2)
        # session_obj.commit()


        ins = o_table.insert ()
        print(data_list)
        ans = connection.execute (ins, data_list)


        ans = True
    except IntegrityError as error:

        print (error)
    except Exception as error:

        print (error)
    finally:
        connection.close ()

    return ans


def pd_insert_db(df):
    data_list = df.to_dict (orient='records')
    # insert_db(data_list, tablename='o32nav')
    insert_direct (data_list, tablename='new_table')

    return


def init_db(path='./insert_db'):
    df_list = get_data (futures_path=path)
    for df in df_list:
        # insert_db(df, tablename='o32nav')
        pd_insert_db (df)
    # df_list = get_data()
    # for df in df_list:
    #     pd_insert_db(df)
    return



# 元类
class ModelMeta(DeclarativeMeta):
    def __new__(cls, product_code, bases, d):
        return DeclarativeMeta.__new__(cls, product_code, bases, d)

    def __init__(self, product_code, bases, d):
        DeclarativeMeta.__init__(self, product_code, bases, d)


#
_Base = declarative_base(metaclass=ModelMeta)


class BaseModel(_Base):
    __abstract__ = True

    # 基类的 _column_name_sets  是为实现的类型
    _column_name_sets = NotImplemented

    def to_dict(self):
        """
        """
        return dict(
            (column_name, getattr(self, column_name, None)) \
            for column_name in self._column_name_sets
        )

    def set_with_dict(self, mdict=None):

        for column_name in self._column_name_sets:
            if column_name in mdict:
                var = mdict[column_name]
                setattr(self, column_name, var)

        return

    @classmethod
    def get_column_name_sets(cls):
        """
        获取 column 的定义的名称(不一定和数据库字段一样)
        """
        return cls._column_name_sets

    __str__ = lambda self: str(self.to_dict())
    __repr__ = lambda self: repr(self.to_dict())


def modelmeta__new__(cls, product_code, bases, namespace, **kwds):
    column_name_sets = set()
    for k, v in namespace.items():
        if getattr(v, '__class__', None) is None:
            continue
        if v.__class__.__name__ == 'Column':
            column_name_sets.add(k)

    # obj = type.__new__(cls, product_code, bases, dict(namespace))
    obj = DeclarativeMeta.__new__(cls, product_code, bases, dict(namespace))
    # update set
    obj._column_name_sets = column_name_sets
    return obj


# modify BaseModel' metatype ModelMeta' __new__ definition
setattr(ModelMeta, '__new__', modelmeta__new__)


class User(BaseModel):
    __tablename__ = 'users'

    id = Column(Integer,Sequence('id_seq'),primary_key=True)
    # product_code = Column(CHAR(30)) # or Column(String(30))
    # created_date = Column(DateTime, default=datetime.utcnow)
    product_code = Column(String(32))
    acc_id = Column(Integer())
    product_name = Column(String(50))
    product_share = Column(Integer())
    manage_asset_value = Column(Numeric(20, 2))
    current_nav = Column(Numeric(12, 2))
    warning_line = Column(Numeric(12, 2))
    liquidation_line= Column(Numeric(12, 2))
    remarks = Column(String(50))
    report_date = Column(String(50))
    last_modified_date = Column(DateTime, default=datetime.datetime.now)
    # Column('created_on', DateTime(), default=datetime.now),
    # Column('time', DateTime, default=datetime.now()),
    # updated_on = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def set_with_dict(self, mdict=None):
        # mdict = {'product_code': 'SH1385', 'acc_id': 900166022, 'product_name': '中信期货金富招商1号',
        #          'product_share': 14000, 'manage_asset_value': 5, 'current_nav': 0, 'warning_line': 0.9,
        #          'liquidation_line': 0.88, 'remarks': '持仓股票停牌，二次清算', 'report_date': 20171208}
        # column_name_list = [
        #     value[0] for value in self._sa_instance_state.attrs.items()
        # ]

        for column_name in self._column_name_sets:
            if column_name in mdict:
                var = mdict[column_name]
                setattr(self, column_name, var)

        return


class O32(BaseModel):
    __tablename__ = 'o32'

    id = Column(Integer,Sequence('o32_id_seq'),primary_key=True)
    product_code = Column(String(32))
    acc_id = Column(Integer())
    product_name = Column(String(50))
    product_share = Column(Integer())
    manage_asset_value = Column(Numeric(20, 2))
    current_nav = Column(Numeric(12, 2))
    warning_line = Column(Numeric(12, 2))
    liquidation_line= Column(Numeric(12, 2))
    remarks = Column(String(50))
    report_date = Column(String(50))
    last_modified_date = Column(DateTime, default=datetime.datetime.now)


class ContractInfo(BaseModel):
    __tablename__ = 'contractinfo'

    id = Column(Integer, Sequence('id_seq'), primary_key=True)
    exchange = Column(String(32))
    symbol = Column(String(32))
    contract = Column(String(32))
    bid1 = Column(Numeric(20, 2))
    mid = Column(Numeric(20, 2))
    ask1 = Column(Numeric(20, 2))
    multiplier = Column(Integer())
    isexpired = Column(Boolean())
    servertime = Column(DateTime, default=datetime.datetime.now)
    preclose = Column(Numeric(12, 2))
    pctchange = Column(Numeric(12, 2))
    lastprice = Column(Numeric(12, 2))
    impliedvol = Column(Numeric(12, 2))
    initialvol = Column(Numeric(12, 2))
    midvol = Column(Numeric(12, 2))
    last_modified_date = Column(DateTime, default=datetime.datetime.now)


def init_db(engine):
    BaseModel.metadata.create_all(engine)

def drop_db(engine):
    BaseModel.metadata.drop_all(engine)

def main():
    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    source_constr = hsfa_constr
    engine = create_engine(source_constr, encoding='gbk', echo=True)
    engine.execute('''declare
  num   number;
begin
  select count(*) into num from user_tables where table_name = upper('users') ;
  if num > 0 then
    execute immediate 'drop table users' ;
  end if;
end;''')
    # BaseModel.metadata.remove(User)
    BaseModel.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()  # class session -> object
    user = User(product_code='a')
    # user.set_with_dict()
    print(user.to_dict())
    session.add(user)
    user = User(product_code='b')
    session.add(user)
    user = User(product_code='b')
    session.add(user)
    user = User(product_code='a')
    session.add(user)
    user = User()
    session.add(user)
    # user = User(product_code='SH1385', acc_id=900166022, product_name='中信期货金富招商1号', product_share=14000,
    # manage_asset_value=5, current_nav=0, warning_line=0.9, liquidation_line=0.88,
    # remarks='持仓股票停牌，二次清算', report_date=20171207)
    session.add(user)

    session.commit()
    path = './insert_db'
    df_list = get_data(futures_path=path)
    for df in df_list:
        data_list = df.to_dict(orient='records')
        session.bulk_insert_mappings(User, data_list)

    wp_file_path = './wp/contract.xlsx'
    wp_xls = pd.ExcelFile(wp_file_path)
    print(wp_xls.sheet_names)
    for sheet_name in wp_xls.sheet_names:

        if sheet_name == 'ContractInfo':
            sheet_df = pd.read_excel(wp_file_path, sheet_name=sheet_name)
            print(sheet_df)
            sheet_df.rename(columns=lambda x: x.strip().lower(), inplace=True)
            old_col_names = sheet_df.columns.tolist()

            str_cols = ['exchange', 'symbol', 'contract', ]
            number_cols = {'bid1', 'mid', 'ask1', 'multiplier',
                           'preclose', 'pctchange', 'lastprice', 'impliedvol', 'initialvol',
                           'midvol',

                           }
            time_cols = ['servertime', ]
            bool_cols = ['isexpired']
            for col in old_col_names:
                print(col)
                if col in number_cols:
                    sheet_df[col] = sheet_df[col].astype('float64')


                    sheet_df[col] = sheet_df[col].fillna(0)
                elif col in bool_cols:
                    sheet_df[col] = sheet_df[col].fillna(False)
                    print(sheet_df[col])
                    sheet_df[col] = sheet_df[col].replace([0, ], [False, ])
                    # sheet_df[col] = sheet_df[col].replace(['FALSE', ], [False, ])
                elif col in time_cols:
                    sheet_df[col] = datetime.datetime.now()
                else:
                    # sheet_df[col] = sheet_df[col].replace({'-': '0', '不设清盘线': '0'})
                    sheet_df[col] = sheet_df[col].fillna('0')
            print(sheet_df)
            data_list = sheet_df.to_dict(orient='records')
            print(data_list)
            print(data_list[0])
            var = ContractInfo()
            test_dict = {'exchange': 'DCE', 'symbol': 'C', 'contract': 'C1701.DCE'}
            var.set_with_dict(test_dict)
            session.add(var)
            session.bulk_insert_mappings(ContractInfo, data_list)

    # init_db()
    session.commit()
    return
if __name__ == '__main__':
    main()