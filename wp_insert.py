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


def readAll(path, fileType):
    files = os.listdir(path)
    # textContainer = []
    excelFileList = []
    for file in files:
        file = path + '/' + file
        if not os.path.isfile(file):
            continue
        if file.endswith(fileType):
            excelFileList.append(file)
            print(file)

    return excelFileList


def get_time(fn=None):
    # fn = '日期：2017-12-7'
    ans = fn.rsplit('：', 1)
    tail_ans = ans[1]
    time = datetime.datetime.strptime(tail_ans, '%Y-%m-%d')
    str = time.strftime('%Y%m%d')
    int_time = int(str)

    return int_time, time


def get_data(futures_path=None, file_type='xlsx'):
    fut_files = readAll(futures_path, file_type)
    df_list = []
    for a_file in fut_files:
        # df = pd.read_csv(a_file, header=0, skipfooter=1, encoding='python', encoding='utf8')
        # pandas的索引函数主要有三种：
        # loc 标签索引，行和列的名称
        # iloc 整型索引（绝对位置索引），绝对意义上的几行几列，起始索引为0
        # ix 是 iloc 和 loc的合体
        # at是loc的快捷方式
        # iat是iloc的快捷方式

        df = pd.read_excel(a_file, header=None, skip_blank_lines=True)
        print(df.iloc[0])
        print(df.iloc[1])
        print(df.iloc[2])

        time_row = df.iloc[1]
        time_row = time_row.dropna().tolist()
        time_str = None
        for element in time_row:
            if '日期' in element:
                time_str = element
        if not time_row:
            return
        int_time, file_time = get_time(time_str)

        clean_df = df[3:]
        clean_df = clean_df.reset_index(drop=True)
        print(clean_df)
        print(df.iloc[2])
        print(df.iloc[2].tolist())
        clean_df.columns = df.iloc[2].tolist()
        clean_df.rename(columns=lambda x: x.strip(), inplace=True)
        number_cols = {'投资者代码', '产品份额', '最新净值', '单位净值', '预警线', '清盘线'}
        old_col_names = clean_df.columns.tolist()
        for col in old_col_names:
            if col in number_cols:
                clean_df[col] = clean_df[col].replace(['-', '不设清盘线'], [0, 0])
                clean_df[col] = clean_df[col].fillna(0)
            else:
                clean_df[col] = clean_df[col].replace({'-': '0', '不设清盘线': '0'})
                clean_df[col] = clean_df[col].fillna('0')

        cols_dict = {'备案号': 'product_code', '投资者代码': 'acc_id', '产品名称': 'product_name',
                     '产品份额': 'product_share', '最新净值': 'manage_asset_value',
                     '单位净值': 'current_nav',
                     '预警线': 'warning_line', '清盘线': 'liquidation_line', '备注': 'remarks'}
        clean_df.rename(columns=lambda x: x.strip(), inplace=True)
        clean_df.rename(columns=cols_dict, inplace=True)
        clean_df['report_date'] = int_time
        # clean_df['id'] = 0
        print(clean_df)
        df_list.append(clean_df)
    return df_list


def insert_wp_sheet(type_dict=None, wp_file_path=None, sheet_name=None, meta=None, session=None):
    tablename = sheet_name.lower()
    # tablename = 'pms_opt'
    # sheet_name = 'PMS_Opt'
    str_cols = type_dict['str_cols']
    number_cols = type_dict['number_cols']
    time_cols = type_dict['time_cols']
    bool_cols = type_dict['bool_cols']

    # str_cols = ['contractname', 'basecontract', ]
    # number_cols = ['futuresmultiplier', 'netposition', 'longposition', 'shortposition', 'stddelta', 'stdgamma',
    #                'stdvega',
    #                'stdtheta', 'stdrho', 'cashdelta', 'cashgamma', 'pctcashgamma', 'pctvega', 'dailytheta',
    #                'marketvalue',
    #                'cashvalue', 'pnl', 'theovalue', 'lastprice']
    # time_cols = []
    # bool_cols = ['isexpired']

    seq_name = tablename + '_id_seq'
    PMS_Opt_mtable = Table(tablename, meta,
                           Column('id', Integer, Sequence(seq_name), primary_key=True),
                           Column('last_modified_date', DateTime, default=datetime.datetime.now),
                           autoload=True)

    class PMS_Opt(object):
        pass

    mapper(PMS_Opt, PMS_Opt_mtable)

    sheet_df = pd.read_excel(wp_file_path, sheet_name=sheet_name, encoding='utf-8')
    print(sheet_df)
    sheet_df.rename(columns=lambda x: x.strip().lower(), inplace=True)
    old_col_names = sheet_df.columns.tolist()

    # sheet_df.to_sql (sheet_name.lower(), engine, if_exists='append', index=False)



    for col in old_col_names:
        print(col)
        if col in number_cols:
            sheet_df[col] = sheet_df[col].astype('float64')
            sheet_df[col] = sheet_df[col].fillna(0)
        elif col in bool_cols:
            print(sheet_df[col])
            sheet_df[col] = sheet_df[col].fillna(False)
            print(sheet_df[col])
        elif col in time_cols:
            sheet_df[col] = datetime.datetime.now()
        else:
            sheet_df[col] = sheet_df[col].astype('str')
            # sheet_df[col] = sheet_df[col].str.encode("utf-8")

            sheet_df[col] = sheet_df[col].fillna('0')
    print(sheet_df)
    # sheet_df = sheet_df.drop(str_cols, axis=1)
    data_list = sheet_df.to_dict(orient='records')
    print(data_list)
    num_rows_deleted = session.query(PMS_Opt).delete()
    print(repr(num_rows_deleted) + " rows deleted!")
    data_list = data_list[0:10]
    session.bulk_insert_mappings(PMS_Opt, data_list)
    session.commit()
    return


def test():
    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    source_constr = hsfa_constr
    engine = create_engine(source_constr, encoding='gbk', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()  # class session -> object
    tablename = 'o32'
    meta = MetaData(bind=engine)
    from sqlalchemy import DateTime
    o_table = Table(tablename.lower(), meta,
                    Column('id', Integer, Sequence('o32_id_seq'), primary_key=True),
                    Column('last_modified_date', DateTime, default=datetime.datetime.now),
                    autoload=True)

    class O32(object):
        pass

    mapper(O32, o_table)

    # path = './insert_db'
    # df_list = get_data(futures_path=path)
    # for df in df_list:
    #     data_list = df.to_dict(orient='records')
    #     session.bulk_insert_mappings(O32, data_list)
    #     session.commit()

    tablename = 'contractinfo'
    seq_name = tablename + '_id_seq'
    mtable = Table(tablename, meta,
                   Column('id', Integer, Sequence(seq_name), primary_key=True),
                   Column('last_modified_date', DateTime, default=datetime.datetime.now),
                   autoload=True)

    class ContractInfo(object):
        pass

    mapper(ContractInfo, mtable)

    tablename = 'pms_opt'
    seq_name = tablename + '_id_seq'
    PMS_Opt_mtable = Table(tablename, meta,
                           Column('id', Integer, Sequence(seq_name), primary_key=True),
                           Column('last_modified_date', DateTime, default=datetime.datetime.now),
                           autoload=True)

    class PMS_Opt(object):
        pass

    mapper(PMS_Opt, PMS_Opt_mtable)

    wp_file_path = './wp/contract.xlsx'
    wp_file_path = './wp/wp2.xlsx'
    wp_xls = pd.ExcelFile(wp_file_path)
    print(wp_xls.sheet_names)
    for sheet_name in wp_xls.sheet_names:
        insert_wp_sheet(wp_file_path=wp_file_path, sheet_name=sheet_name, meta=meta, session=session)

    for sheet_name in wp_xls.sheet_names:

        if sheet_name == 'ContractInfo':
            continue
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
            session.bulk_insert_mappings(ContractInfo, data_list)
            session.commit()
        elif sheet_name == 'PMS_Opt':
            sheet_df = pd.read_excel(wp_file_path, sheet_name=sheet_name)
            print(sheet_df)
            sheet_df.rename(columns=lambda x: x.strip().lower(), inplace=True)
            old_col_names = sheet_df.columns.tolist()
            # sheet_df.to_sql (sheet_name.lower(), engine, if_exists='append', index=False)


            str_cols = ['contractname', 'basecontract', ]
            number_cols = ['futuresmultiplier', 'netposition', 'longposition', 'shortposition', 'stddelta', 'stdgamma',
                           'stdvega',
                           'stdtheta', 'stdrho', 'cashdelta', 'cashgamma', 'pctcashgamma', 'pctvega', 'dailytheta',
                           'marketvalue',
                           'cashvalue', 'pnl', 'theovalue', 'lastprice']
            time_cols = []
            bool_cols = ['isexpired']
            for col in old_col_names:
                # col = col.lower()
                print(col)
                if col in number_cols:
                    sheet_df[col] = sheet_df[col].astype('float64')

                    sheet_df[col] = sheet_df[col].fillna(0)
                elif col in bool_cols:
                    print(sheet_df[col])
                    sheet_df[col] = sheet_df[col].fillna(False)
                    print(sheet_df[col])
                    # sheet_df[col] = sheet_df[col].replace([0, ], [False, ])
                    # sheet_df[col] = sheet_df[col].replace(['FALSE', 'True'], [False, True])
                elif col in time_cols:
                    sheet_df[col] = datetime.datetime.now()
                else:
                    # sheet_df[col] = sheet_df[col].replace({'-': '0', '不设清盘线': '0'})
                    sheet_df[col] = sheet_df[col].fillna('0')
            print(sheet_df)
            # sheet_df = sheet_df.drop(str_cols, axis=1)
            data_list = sheet_df.to_dict(orient='records')
            print(data_list)
            print(data_list[0])
            # import io
            # import sys
            # sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
            # var = data_list[0]
            # var['contractname'] = var['contractname'].replace(u'\xa0', u' ')
            # var['basecontract'] = var['basecontract'].replace(u'\xa0', u' ')
            for var in data_list:
                var['contractname'] = var['contractname'].replace(u'\xa0', u' ')
                var['basecontract'] = var['basecontract'].replace(u'\xa0', u' ')
                print(var)
            session.bulk_insert_mappings(PMS_Opt, data_list)
            session.commit()
        else:
            pass





def main():
    type_config = {}

    type_config['ContractInfo'] = {
        'number_cols': ['bid1', 'mid', 'ask1', 'multiplier', 'preclose', 'pctchange', 'lastprice',
                        'impliedvol', 'initialvol', 'midvol'],
        'str_cols': ['contract'],
        'time_cols': ['servertime'],
        'bool_cols': ['isexpired']
    }
    type_config['OptionInfo'] = {
        'number_cols': ['totalamount', 'multiplier', 'lot', 'orgunderlying', 'strike', 'ref1', 'barrier',
                        'strike3', 'geomorarith', 'digitalcash', 'rebate', 'underlying', 'avgdays',
                        'avgsofar', 'riskfree', 'dividend', 'impliedvol', 'premiumprice', 'theovalue',
                        'stddelta', 'stdgamma', 'stdvega', 'stdtheta', 'stdrho', 'realizedpnl',
                        'beginpremium', 'endpremium', 'totalpremium', 'nominalprice', 'netdeposit'],
        'str_cols': ['optionname', 'basecontract', 'client', 'direction', 'remarks'],
        'time_cols': ['dealstartdate', 'avgstartdate', 'expiry', 'timetostartavg', 'timetoexpiry', 'orgtimetoexpiry'],
        'bool_cols': []
    }
    type_config['PMS_Opt'] = {
        'str_cols': ['contractname', 'basecontract', ],
        'number_cols': ['futuresmultiplier', 'netposition', 'longposition', 'shortposition', 'stddelta', 'stdgamma',
                        'stdvega',
                        'stdtheta', 'stdrho', 'cashdelta', 'cashgamma', 'pctcashgamma', 'pctvega', 'dailytheta',
                        'marketvalue',
                        'cashvalue', 'pnl', 'theovalue', 'lastprice'],
        'time_cols': [],
        'bool_cols': ['isexpired']
    }
    type_config['PMS_Fut'] = {
        'number_cols': ['netposition', 'longposition', 'shortposition', 'stddelta', 'cashdelta', 'marketvalue',
                        'cashvalue', 'pnl', 'lastprice', 'multiplier'],
        'str_cols': ['contractname'],
        'time_cols': [],
        'bool_cols': ['isexpired']
    }
    type_config['TradingBlotter'] = {
        'number_cols': ['amount', 'tradeprice'],
        'str_cols': ['contractname', 'contracttype', 'trackingid', 'side', 'positioneffect'],
        'time_cols': ['bookingdate', 'bookingtime', 'tradetime'],
        'bool_cols': []
    }

    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    source_constr = hsfa_constr
    engine = create_engine(source_constr, encoding='gbk', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()  # class session -> object
    meta = MetaData(bind=engine)

    wp_file_path = './wp/contract.xlsx'
    wp_file_path = './wp/wp2.xlsx'
    wp_xls = pd.ExcelFile(wp_file_path)
    print(wp_xls.sheet_names)

    wp_sheet_names = ['ContractInfo', 'PMS_Opt', 'PMS_Fut', 'TradingBlotter']
    wp_sheet_names = ['PMS_Fut', 'TradingBlotter']

    for sheet_name in wp_xls.sheet_names:
        var = wp_xls.parse(sheet_name)
        # type_dict=None, wp_file_path=None, sheet_name=None, meta=None, session=None
        if sheet_name not in wp_sheet_names:
            continue
        print(sheet_name)
        insert_wp_sheet(type_dict=type_config[sheet_name], wp_file_path=wp_file_path, sheet_name=sheet_name, meta=meta, session=session)
        # break


if __name__ == '__main__':
    main()
