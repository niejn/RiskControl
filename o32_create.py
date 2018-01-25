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
      end;'''.format(name='o32'))
    init_db(engine=engine)
    Session = sessionmaker(bind=engine)
    session = Session()  # class session -> object
    o32 = O32(product_code='a')
    print(o32.to_dict())
    session.add(o32)
    o32 = O32(product_code='b')
    session.add(o32)
    o32 = O32(product_code='b')
    session.add(o32)
    o32 = O32(product_code='a')
    session.add(o32)
    o32 = O32()
    session.add(o32)

    session.commit()



    return
if __name__ == '__main__':
    main()