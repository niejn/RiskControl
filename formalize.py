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
    str = 'OptionName	OptionType	Side	CallPut	BaseContract	TotalAmount	' \
          '	Lot	OrgUnderlying	Strike	Ref1	Barrier	Strike3	GeomOrArith	' \
          'DigitalCash	Rebate	Underlying	DealStartDate	AvgStartDate	Expiry	TimeToStartAvg	' \
          'TimeToExpiry	OrgTimeToExpiry	AvgDays	AvgSofar	RiskFree	Dividend	' \
          'ImpliedVol	PremiumPrice	TheoValue	StdDelta	StdGamma	StdVega	StdTheta	' \
          'StdRho	RealizedPnL	IsExpired	BeginPremium	EndPremium	TotalPremium	NominalPrice	Client	Status	EndDate	OptionCloseName'

    ans = str.split()
    print(len(ans))
    str2 = '''OptionName
OptionType
Side
CallPut
BaseContract
TotalAmount
Multiplier
Lot
OrgUnderlying
Strike
Ref1
Barrier
Strike3
GeomOrArith
DigitalCash
Rebate
Underlying
DealStartDate
AvgStartDate
Expiry
TimeToStartAvg
TimeToExpiry
OrgTimeToExpiry
AvgDays
AvgSofar
RiskFree
Dividend
ImpliedVol
PremiumPrice
TheoValue
StdDelta
StdGamma
StdVega
StdTheta
StdRho
RealizedPnL
IsExpired
BeginPremium
EndPremium
TotalPremium
NominalPrice
Client
'''
    ans2 = str2.split()
    for col in ans:
        if col not in ans2:
            print(col)
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

    cols = str.split('\n')
    new_cols = []
    str_col_list = []
    float_col_list = []
    time_col_list = []
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

            if col_type in ['Float', 'Integer', 'Numeric(12, 2)']:
                float_col_list.append(col_name)
            elif col_type in ['String(40)', 'String', ]:
                str_col_list.append(col_name)
            elif col_type in ['DateTime', ]:
                time_col_list.append(col_name)
            else:
                print(col_type)
                print(col_name)
                # pass

        else:
            print("No match!!")
            print(col)
    ans_str = ''
    for col in new_cols:
        ans_str = ans_str + col + '\n'
    print(ans_str)
    print(new_cols)

    # 'str_cols': ['contractname', 'basecontract', ],
    # 'number_cols': ['futuresmultiplier', 'netposition', 'longposition', 'shortposition', 'stddelta', 'stdgamma',
    #                 'stdvega',
    #                 'stdtheta', 'stdrho', 'cashdelta', 'cashgamma', 'pctcashgamma', 'pctvega', 'dailytheta',
    #                 'marketvalue',
    #                 'cashvalue', 'pnl', 'theovalue', 'lastprice'],
    # 'time_cols': [],
    # 'bool_cols': ['isexpired']

    print('-' * 60)
    print("'number_cols': " + repr(float_col_list) + ',')
    print("'str_cols': " + repr(str_col_list) + ',')
    print("'time_cols': " + repr(time_col_list) + ',')

    # print("'str_cols': " + str_col_list)
    # print("'time_cols': " + time_col_list)
    return
if __name__ == '__main__':
    main()