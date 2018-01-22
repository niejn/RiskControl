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


class O32(BaseModel):
    __tablename__ = 'o32'

    id = Column(Integer,Sequence('id_seq'),primary_key=True)
    # product_code = Column(CHAR(30)) # or Column(String(30))
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
    last_modified_date = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    # updated_on = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # def set_vals_dict(self, vdict):
    #     self.product_code = vdict['product_code']
    #     self.acc_id = vdict['acc_id']
    #     self.product_code = vdict['product_code']
    #     self.product_code = vdict['product_code']
    #     self.product_code = vdict['product_code']
    #     self.product_code = vdict['product_code']
    #     self.product_code = vdict['product_code']
    #
    #     product_code = 'SH1385', acc_id = 900166022, product_name = '中信期货金富招商1号', product_share = 14000,
    #     manage_asset_value = 5, current_nav = 0, warning_line = 0.9, liquidation_line = 0.88,
    #     remarks = '持仓股票停牌，二次清算', report_date = 20171207
    #     return

    # def get_values(self):
    #
    #     for field in self._meta.fields:
    #         name = field.verbose_name
    #
    #         value = getattr(instance, field.name)
    #         print('%s: %s' % (name, value))
    # def __init__(self, product_code='SH1385', acc_id=900166022, product_name='中信期货金富招商1号',
    #              product_share=14000,
    # manage_asset_value=5, current_nav=0, warning_line=0.9, liquidation_line=0.88,
    # remarks='持仓股票停牌，二次清算', report_date=20171207):
    #     self.product_code = product_code
        


# {product_code: 'SH1385', acc_id: 900166022, product_name: '中信期货金富招商1号', product_share: 14000,
# manage_asset_value: 5, current_nav: 0, warning_line: 0.9, liquidation_line: 0.88,
# remarks: '持仓股票停牌，二次清算', report_date: 20171207}

# init_db()
def main():
    engine.execute('''
    declare
        num   number;
    begin
        select count(*) into num from user_tables where table_name = upper('o32') ;
        if num > 0 then
            execute immediate 'drop table o32' ;
        end if;
    end;''')
    # BaseModel.metadata.remove(O32)
    init_db()
    Session = sessionmaker(bind=engine)
    session = Session()  # class session -> object
    o32 = O32(product_code='a')
    # o32.get_values()
    session.add(o32)
    o32 = O32(product_code='b')
    session.add(o32)
    o32 = O32(product_code='b')
    session.add(o32)
    o32 = O32(product_code='a')
    session.add(o32)
    o32 = O32()
    session.add(o32)
    o32 = O32(product_code='SH1385', acc_id=900166022, product_name='中信期货金富招商1号', product_share=14000,
    manage_asset_value=5, current_nav=0, warning_line=0.9, liquidation_line=0.88,
    remarks='持仓股票停牌，二次清算', report_date=20171207)
    session.add(o32)

    session.commit()
    return
if __name__ == '__main__':
    main()