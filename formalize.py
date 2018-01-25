import re

    # Column('BookingDate',               DateTime),
    # Column('BookingTime',               DateTime),
    # Column('ContractName',              String(40)),
    # Column('ContractType',              String(40)),
    # Column('TrackingID',                 String(40)),
    # Column('TradeTime',                  DateTime),
    # Column('Amount',                      Float),
    # Column('Side',                        String(40)),
    # Column('PositionEffect',            String(40)),
    # Column('TradePrice',                 Float),
def main():
    # product_code = Column(String(32))
    str = '''
    Column('BookingDate',               DateTime),
    Column('BookingTime',               DateTime),
    Column('ContractName',              String(40)),
    Column('ContractType',              String(40)),
    Column('TrackingID',                 String(40)),
    Column('TradeTime',                  DateTime),
    Column('Amount',                      Float),
    Column('Side',                        String(40)),
    Column('PositionEffect',            String(40)),
    Column('TradePrice',                 Float),
    '''
    '''servertime = Column(DateTime, default=datetime.datetime.now)
    preclose = Column(Numeric(12, 2))
    pctchange = Column(Numeric(12, 2))
    lastprice = Column(Numeric(12, 2))
    impliedvol = Column(Numeric(12, 2))
    initialvol = Column(Numeric(12, 2))
    midvol = Column(Numeric(12, 2))'''
    cols = str.split('\n')
    new_cols = []
    for col in cols:
        col = col.strip()
        matchObj = re.match(r"Column\('(.*)',\s*([\S]*)\),", col, re.M | re.I)
        if matchObj:
            # print("matchObj.group() : ", matchObj.group())
            # print("matchObj.group(1) : ", matchObj.group(1))
            # print("matchObj.group(2) : ", matchObj.group(2))
            col_name = matchObj.group(1)
            col_type = matchObj.group(2)
            if col_type in ['Float']:
                col_type = 'Numeric(12, 2)'
            formal_str = '{col_name} = Column({col_type})'.format(col_name=col_name.lower(), col_type=col_type)
            print(formal_str)
            new_cols.append(formal_str)


        else:
            print("No match!!")
            print(col)
    ans_str = ''
    for col in new_cols:
        ans_str = ans_str + col + '\n'
    print(ans_str)
    print(new_cols)
    return
if __name__ == '__main__':
    main()