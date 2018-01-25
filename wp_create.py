from sqlalchemy import ForeignKey, Sequence, Boolean
from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import CHAR, Integer, String
from sqlalchemy.ext.declarative import declarative_base
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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper


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
    obj = DeclarativeMeta.__new__(cls, product_code, bases, dict(namespace))
    obj._column_name_sets = column_name_sets
    return obj


setattr(ModelMeta, '__new__', modelmeta__new__)


class ContractInfo(BaseModel):
    __tablename__ = 'contractinfo'

    id = Column(Integer, Sequence('id_seq'), primary_key=True)
    exchange = Column(String(32))
    symbol = Column(String(32))
    contract = Column(String(32))
    bid1 = Column(Numeric(20, 2))
    mid = Column(Numeric(20, 2))
    ask1 = Column(Numeric(20, 2))
    multiplier = Column(Integer)
    isexpired = Column(Boolean)
    servertime = Column(DateTime, default=datetime.datetime.now)
    preclose = Column(Numeric(12, 2))
    pctchange = Column(Numeric(12, 2))
    lastprice = Column(Numeric(12, 2))
    impliedvol = Column(Numeric(12, 2))
    initialvol = Column(Numeric(12, 2))
    midvol = Column(Numeric(12, 2))
    last_modified_date = Column(DateTime, default=datetime.datetime.now)


class OptionInfo(BaseModel):
    __tablename__ = 'optioninfo'
    id = Column(Integer, Sequence('optioninfo_id_seq'), primary_key=True)
    last_modified_date = Column(DateTime, default=datetime.datetime.now)

    optionname = Column(String(40))
    optiontype = Column(String(20))
    side = Column(Boolean)
    callput = Column(Boolean)
    basecontract = Column(String(40))
    totalamount = Column(Numeric(12, 2))
    multiplier = Column(Integer)
    lot = Column(Numeric(12, 2))
    orgunderlying = Column(Numeric(12, 2))
    strike = Column(Numeric(12, 2))
    ref1 = Column(Numeric(12, 2))
    barrier = Column(Numeric(12, 2))
    strike3 = Column(Numeric(12, 2))
    geomorarith = Column(Numeric(12, 2))
    digitalcash = Column(Numeric(12, 2))
    rebate = Column(Numeric(12, 2))
    underlying = Column(Numeric(12, 2))
    dealstartdate = Column(DateTime)
    avgstartdate = Column(DateTime)
    expiry = Column(DateTime)
    timetostartavg = Column(DateTime)
    timetoexpiry = Column(DateTime)
    orgtimetoexpiry = Column(DateTime)
    avgdays = Column(Integer)
    avgsofar = Column(Numeric(12, 2))
    riskfree = Column(Numeric(12, 2))
    dividend = Column(Numeric(12, 2))
    impliedvol = Column(Numeric(12, 2))
    premiumprice = Column(Numeric(12, 2))
    theovalue = Column(Numeric(12, 2))
    stddelta = Column(Numeric(12, 2))
    stdgamma = Column(Numeric(12, 2))
    stdvega = Column(Numeric(12, 2))
    stdtheta = Column(Numeric(12, 2))
    stdrho = Column(Numeric(12, 2))
    realizedpnl = Column(Numeric(12, 2))
    isexpired = Column(Boolean)
    beginpremium = Column(Numeric(12, 2))
    endpremium = Column(Numeric(12, 2))
    totalpremium = Column(Numeric(12, 2))
    nominalprice = Column(Numeric(12, 2))
    client = Column(String(40))
    netdeposit = Column(Numeric(12, 2))
    direction = Column(String(40))
    remarks = Column(String(40))




class PMS_Opt(BaseModel):
    __tablename__ = 'pms_opt'
    id = Column(Integer, Sequence('pms_opt_id_seq'), primary_key=True)
    last_modified_date = Column(DateTime, default=datetime.datetime.now)

    contractname = Column(String(40))
    basecontract = Column(String(40))
    futuresmultiplier = Column(Integer)
    netposition = Column(Integer)
    longposition = Column(Integer)
    shortposition = Column(Integer)
    stddelta = Column(Numeric(12, 2))
    stdgamma = Column(Numeric(12, 2))
    stdvega = Column(Numeric(12, 2))
    stdtheta = Column(Numeric(12, 2))
    stdrho = Column(Numeric(12, 2))
    cashdelta = Column(Numeric(12, 2))
    cashgamma = Column(Numeric(12, 2))
    pctcashgamma = Column(Numeric(12, 2))
    pctvega = Column(Numeric(12, 2))
    dailytheta = Column(Numeric(12, 2))
    marketvalue = Column(Numeric(12, 2))
    cashvalue = Column(Numeric(12, 2))
    pnl = Column(Numeric(12, 2))
    theovalue = Column(Numeric(12, 2))
    lastprice = Column(Numeric(12, 2))
    isexpired = Column(Boolean)


class PMS_Fut(BaseModel):
    __tablename__ = 'pms_fut'
    id = Column(Integer, Sequence('pms_fut_id_seq'), primary_key=True)
    last_modified_date = Column(DateTime, default=datetime.datetime.now)

    contractname = Column(String(40))
    netposition = Column(Numeric(12, 2))
    longposition = Column(Numeric(12, 2))
    shortposition = Column(Numeric(12, 2))
    stddelta = Column(Integer)
    cashdelta = Column(Integer)
    marketvalue = Column(Numeric(12, 2))
    cashvalue = Column(Numeric(12, 2))
    pnl = Column(Numeric(12, 2))
    lastprice = Column(Numeric(12, 2))
    isexpired = Column(String(40))
    multiplier = Column(Integer)


class TradingBlotter(BaseModel):
    __tablename__ = 'tradingblotter'
    id = Column(Integer, Sequence('tradingblotter_id_seq'), primary_key=True)
    last_modified_date = Column(DateTime, default=datetime.datetime.now)

    bookingdate = Column(DateTime)
    bookingtime = Column(DateTime)
    contractname = Column(String(40))
    contracttype = Column(String(40))
    trackingid = Column(String(40))
    tradetime = Column(DateTime)
    amount = Column(Numeric(12, 2))
    side = Column(String(40))
    positioneffect = Column(String(40))
    tradeprice = Column(Numeric(12, 2))


def init_db(engine):
    BaseModel.metadata.create_all(engine)


def drop_db(engine):
    BaseModel.metadata.drop_all(engine)


def main():
    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    source_constr = hsfa_constr
    engine = create_engine(source_constr, encoding='gbk', echo=True)
    engine.execute('''
      declare
          num   number;
      begin
          select count(*) into num from user_tables where table_name = upper('{name}') ;
      if num > 0 then
           execute immediate 'drop table {name}' ;
      end if;
      end;'''.format(name='contractinfo'))
    init_db(engine=engine)
    Session = sessionmaker(bind=engine)
    session = Session()  # class session -> object

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


    session.commit()

    return


if __name__ == '__main__':
    main()