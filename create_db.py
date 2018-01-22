#encoding:utf-8
from sqlalchemy import MetaData, Sequence
from datetime import datetime
from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, DateTime
from sqlalchemy import create_engine
import sqlalchemy
# Column('date', Integer(), primary_key=True),
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.ddl import CreateTable, CreateSequence, DropTable
Base = declarative_base()
metadata = MetaData()
# engine = create_engine('sqlite:///:memory:')
# engine = create_engine('sqlite:///test1.sqlite')
trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
source_constr = hsfa_constr
engine = create_engine(source_constr, encoding='gbk', echo=True)

# 营业部 department
# 业务员 salesman
# 客户姓名	acc_name
# 客户号	acc_id
# 合约品种	future_id
# 成交手数 trading_volume
# 成交金额 turnover
# 日期 date

# Trades = Table('trades', metadata,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('acc_id', Integer(), index=True),
#     Column('date', Integer()),
#     Column('acc_name', String(50)),
#     Column('trading_volume', Integer()),
#     Column('turnover', Integer()),
#     Column('future_id', String(50)),
#     Column('department', String(50)),
#     Column('salesman', String(50)),
#
# )

# 备案号product_code	 投资者代码acc_id	产品名称product_name	产品份额product_share	最新净值manage_asset_value
# 单位净值current_nav
# 预警线warning_line	清盘线 Liquidation line	备注remarks
# 日期 report_date
# product_id acc_id product_name
id_seq = Sequence('id_seq', metadata=metadata, start=0, minvalue=0, increment=1)
# a function which counts upwards
i = 0
def mydefault():
    global i
    i += 1
    return i

# t = Table("mytable", meta,
#     Column('id', Integer, primary_key=True, default=mydefault),
# )
O32 = Table('o32nav', metadata,
    # Column('id', Integer, server_default="0"),
    Column('id', Integer, Sequence('id_seq'),primary_key=True),
    # Column('id', Integer, primary_key=True, default=id_seq.next_value),
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
    Column('time',DateTime,default=datetime.now()),
    Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)

)

table = Table("cartitems", metadata,
    Column(
        "cart_id",
        Integer,
        Sequence('cart_id_seq', metadata=metadata), primary_key=True),
    Column("description", String(40)),
    Column("createdate", DateTime())
)


# cart_id_seq = Sequence('cart_id_seq', metadata=meta)
# table = Table("cartitems", meta,
#     Column(
#         "cart_id", Integer, cart_id_seq,
#         server_default=cart_id_seq.next_value(), primary_key=True),
#     Column("description", String(40)),
#     Column("createdate", DateTime())
# )
# class CartItem(Base):
#     __tablename__ = 'cartitems'
#
#     cart_id_seq = Sequence('cart_id_seq', metadata=Base.metadata)
#     cart_id = Column(
#         Integer, cart_id_seq,
#         server_default=cart_id_seq.next_value(), primary_key=True)
#     description = Column(String(40))
#     createdate = Column(DateTime)
# from datetime import datetime
# from sqlalchemy import DateTime
# users = Table('users', metadata,
# Column('user_id', Integer(), primary_key=True),
# Column('username', String(15), nullable=False, unique=True),
# Column('email_address', String(255), nullable=False),
# Column('phone', String(20), nullable=False),
# Column('password', String(25), nullable=False),
# Column('created_on', DateTime(), default=datetime.now),
# Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)
# )

# Account	Name	Trading Volume		increase	growth_rate	Corporate_trade_vol
# Corporate occupation ratio	market_trade_vol	marketoccupation ratio
# Base.metadata.create_all()
# engine.execute(CreateTable(O32))
# engine.execute(CreateSequence(Sequence('o32_id_seq')))

# class User(Base):
#     __tablename__ = 'users'
#     id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
#     name = Column(String(50))
#     fullname = Column(String(50))
#     password = Column(String(12))
#
#     def __repr__(self):
#         return "<User(name='%s', fullname='%s', password='%s')>" % (
#                                 self.name, self.fullname, self.password)

engine.execute("DROP TABLE o32nav")
# metadata.remove(table=)
ans = metadata.create_all(engine)
# ans = metadata.drop_all()

print(ans)