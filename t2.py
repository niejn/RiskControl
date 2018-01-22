import sqlalchemy as sa
import datetime

class BaseMixin(object):

  __table_args__ = {'mysql_engine': 'InnoDB'}

  id = sa.Column(sa.Integer, primary_key=True)
  created_at = sa.Column('created_at', sa.DateTime, nullable=False)
  updated_at = sa.Column('updated_at', sa.DateTime, nullable=False)

  @staticmethod
  def create_time(mapper, connection, instance):
     now = datetime.datetime.utcnow()
     instance.created_at = now
     instance.updated_at = now

  @staticmethod
  def update_time(mapper, connection, instance):
     now = datetime.datetime.utcnow()
     instance.updated_at = now

  @classmethod
  def register(cls):
     sa.event.listen(cls, 'before_insert', cls.create_time)
     sa.event.listen(cls, 'before_update', cls.update_time)

from sqlalchemy import create_engine

# 数据库连接字符串
DB_CONNECT_STRING = 'sqlite:///:memory:'

# 创建数据库引擎,echo为True,会打印所有的sql语句
engine = create_engine(DB_CONNECT_STRING, echo=True)

# 创建一个connection，这里的使用方式与python自带的sqlite的使用方式类似
with engine.connect() as con:
    # 执行sql语句，如果是增删改，则直接生效，不需要commit
    rs = con.execute('SELECT 5')
    data = rs.fetchone()[0]
    print("Data: %s" % data)