创建表格，初始化数据库
------------------------------------------------------------------------------
Exchange	Symbol	Contract	Bid1
Mid	Ask1	Multiplier	IsExpired	ServerTime	PreClose
PctChange	LastPrice	ClientVol	InitialVol	MidVol

ContractInfo = Table(
    'ContractInfo',
    metadata,
    # Column('EXCHANGE', String(20)), #primary_key = True),
    Column('Exchange',  String(20)),
    Column('Symbol',    String(20)),
    Column('Contract',  String(40)),
    Column('Bid1',      Float),
    Column('Mid',       Float),
    Column('Ask1',      Float),
    Column('Multiplier',Integer),
    Column('IsExpired', Boolean),
    Column('ServerTime',DateTime),
    Column('PreClose',  Float),
    Column('PctChange', Float),
    Column('LastPrice', Float),
    Column('ImpliedVol',Float),
    Column('InitialVol',Float),
    Column('MidVol',    Float),
)

#------------------------------------------------------------------------------


OptionInfo = Table(
    'OptionInfo',
    metadata,
    Column('OptionName',                String(40)),
    Column('OptionType',                String(20)),
    Column('Side',                       Boolean),
    Column('CallPut',                   Boolean),
    Column('BaseContract',              String(40)),
    Column('TotalAmount',               Float),
    Column('Multiplier',                Integer),
    Column('Lot',                        Float),
    Column('OrgUnderlying',            Float),
    Column('Strike',                    Float),
    Column('Ref1',                      Float),
    Column('Barrier',                   Float),
    Column('Strike3',                   Float),
    Column('GeomOrArith',               Float),
    Column('DigitalCash',               Float),
    Column('Rebate',                     Float),
    Column('Underlying',                 Float),
    Column('DealStartDate',             DateTime),
    Column('AvgStartDate',              DateTime),
    Column('Expiry',                     DateTime),
    Column('TimeToStartAvg',            DateTime),
    Column('TimeToExpiry',              DateTime),
    Column('OrgTimeToExpiry',           DateTime),
    Column('AvgDays',                    Integer),
    Column('AvgSofar',                   Float),
    Column('RiskFree',                   Float),
    Column('Dividend',                   Float),
    Column('ImpliedVol',                 Float),
    Column('PremiumPrice',               Float),
    Column('TheoValue',                   Float),
    Column('StdDelta',                    Float),
    Column('StdGamma',                    Float),
    Column('StdVega',                     Float),
    Column('StdTheta',                    Float),
    Column('StdRho',                      Float),
    Column('RealizedPnL',                Float),
    Column('IsExpired',                  Boolean),
    Column('BeginPremium',               Float),
    Column('EndPremium',                 Float),
    Column('TotalPremium',               Float),
    Column('NominalPrice',               Float),
    Column('Client',                      String(40)),
    Column('净入金',                       Float),
    Column('方向',                         String(40)),
    Column('备注',                         String(40)),
)


#------------------------------------------------------------------------------

PMS_Opt = Table(
    'PMS_Opt',
    metadata,
    Column('ContractName',              String(40)),
    Column('BaseContract',              String(40)),
    Column('FuturesMultiplier',        Integer),
    Column('NetPosition',               Integer),
    Column('LongPosition',              Integer),
    Column('ShortPosition',             Integer),
    Column('StdDelta',                   Float),
    Column('StdGamma',                   Float),
    Column('StdVega',                    Float),
    Column('StdTheta',                   Float),
    Column('StdRho',                     Float),
    Column('CashDelta',                  Float),
    Column('CashGamma',                  Float),
    Column('PctCashGamma',              Float),
    Column('PctVega',                    Float),
    Column('DailyTheta',                 Float),
    Column('MarketValue',                Float),
    Column('CashValue',                  Float),
    Column('PnL',                         Float),
    Column('TheoValue',                  Float),
    Column('LastPrice',                  Float),
    Column('IsExpired',                  String(40)),
)


#------------------------------------------------------------------------------

PMS_Fut = Table(
    'PMS_Fut',
    metadata,
    Column('ContractName',              String(40)),
    Column('NetPosition',               Float),
    Column('LongPosition',              Float),
    Column('ShortPosition',             Float),
    Column('StdDelta',                   Integer),
    Column('CashDelta',                  Integer),
    Column('MarketValue',                Float),
    Column('CashValue',                  Float),
    Column('PnL',                         Float),
    Column('LastPrice',                  Float),
    Column('IsExpired',                  String(40)),
    Column('Multiplier',                 Integer),
)


#------------------------------------------------------------------------------
Exchange	Symbol	Contract	Bid1	Mid	Ask1	Multiplier	IsExpired	ServerTime	PreClose	PctChange	LastPrice	ClientVol	InitialVol	MidVol

TradingBlotter = Table(
    'TradingBlotter',
    metadata,
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
)

------------------------------------------------------------------------------