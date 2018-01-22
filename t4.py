
# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  @file     models/base.py
#  @author   kaka_ace <xiang.ace@gmail.com>
#  @date
#  @brief
#
from sqlalchemy import ForeignKey, Sequence

from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import CHAR, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from datetime import *
from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, DateTime
from sqlalchemy import ForeignKey, Sequence

from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import CHAR, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from datetime import *
from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import (
    declarative_base,
    DeclarativeMeta,
)

from sqlalchemy.orm import aliased


# 元类
class ModelMeta(DeclarativeMeta):
    def __new__(cls, name, bases, d):
        return DeclarativeMeta.__new__(cls, name, bases, d)

    def __init__(self, name, bases, d):
        DeclarativeMeta.__init__(self, name, bases, d)


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

    @classmethod
    def get_column_name_sets(cls):
        """
        获取 column 的定义的名称(不一定和数据库字段一样)
        """
        return cls._column_name_sets

    __str__ = lambda self: str(self.to_dict())
    __repr__ = lambda self: repr(self.to_dict())


def modelmeta__new__(cls, name, bases, namespace, **kwds):
    column_name_sets = set()
    for k, v in namespace.items():
        if getattr(v, '__class__', None) is None:
            continue
        if v.__class__.__name__ == 'Column':
            column_name_sets.add(k)

    # obj = type.__new__(cls, name, bases, dict(namespace))
    obj = DeclarativeMeta.__new__(cls, name, bases, dict(namespace))
    # update set
    obj._column_name_sets = column_name_sets
    return obj


# modify BaseModel' metatype ModelMeta' __new__ definition
setattr(ModelMeta, '__new__', modelmeta__new__)


class User(BaseModel):
    __tablename__ = 'users'

    id = Column(Integer,Sequence('id_seq'),primary_key=True)
    name = Column(CHAR(30)) # or Column(String(30))
    created_date = Column(DateTime, default=datetime.utcnow)
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
    last_modified_date = Column(DateTime, default=datetime.now)
    # Column('created_on', DateTime(), default=datetime.now),
    # Column('time', DateTime, default=datetime.now()),
    updated_on = Column(DateTime, default=datetime.now, onupdate=datetime.now)



def main():
    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    source_constr = hsfa_constr
    engine = create_engine(source_constr, encoding='gbk', echo=True)
    engine.execute("DROP TABLE users")
    # BaseModel.metadata.remove(User)

    Session = sessionmaker(bind=engine)
    session = Session()  # class session -> object
    user = User(name='a')
    print(user.to_dict())
    session.add(user)
    user = User(name='b')
    session.add(user)
    user = User(name='b')
    session.add(user)
    user = User(name='a')
    session.add(user)
    user = User()
    session.add(user)
    user = User(product_code='SH1385', acc_id=900166022, product_name='中信期货金富招商1号', product_share=14000,
    manage_asset_value=5, current_nav=0, warning_line=0.9, liquidation_line=0.88,
    remarks='持仓股票停牌，二次清算', report_date=20171207)
    session.add(user)

    session.commit()
    return
if __name__ == '__main__':
    main()