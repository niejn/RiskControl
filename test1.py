# from sqlalchemy import Column, Integer, String
# from sqlalchemy import ForeignKey
# from sqlalchemy.orm import relationship
# from Models import Base
#
# class User(Base):
#     __tablename__ = 'users'
#     id = Column('id', Integer, primary_key=True, autoincrement=True)
#     name = Column('name', String(50))
#     age = Column('age', Integer)
#
#     # 添加角色id外键(关联到Role表的id属性)
#     role_id = Column('role_id', Integer, ForeignKey('roles.id'))
#     # 添加同表外键
#     second_role_id = Column('second_role_id', Integer, ForeignKey('roles.id'))
#
#     # 添加关系属性，关联到role_id外键上
#     role = relationship('Role', foreign_keys='User.role_id', backref='User_role_id')
#     # 添加关系属性，关联到second_role_id外键上
#     second_role = relationship('Role', foreign_keys='User.second_role_id', backref='User_second_role_id')
from sqlalchemy import ForeignKey, Sequence

from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import CHAR, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from datetime import *
from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, DateTime
BaseModel = declarative_base()
trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
source_constr = hsfa_constr
engine = create_engine(source_constr, encoding='gbk', echo=True)
def init_db():
    BaseModel.metadata.create_all(engine)

def drop_db():
    BaseModel.metadata.drop_all(engine)


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


    def __init__(self, name, fullname, password):
        self.name = name
        self.fullname = fullname
        self.password = password


# {product_code: 'SH1385', acc_id: 900166022, product_name: '中信期货金富招商1号', product_share: 14000,
# manage_asset_value: 5, current_nav: 0, warning_line: 0.9, liquidation_line: 0.88,
# remarks: '持仓股票停牌，二次清算', report_date: 20171207}

# init_db()
def main():
    engine.execute("DROP TABLE users")
    # BaseModel.metadata.remove(User)
    init_db()
    Session = sessionmaker(bind=engine)
    session = Session()  # class session -> object
    user = User(name='a')
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