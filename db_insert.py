import datetime
import os

import math
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, Column, Integer, Sequence
# metadata = MetaData()
# engine = create_engine('sqlite:///trades.sqlite')
# trades = Table('trades', metadata, autoload=True, autoload_with=engine)
#
# ins = trades.insert()
# # result = connection.execute(ins, inventory_list)
# connection = engine.connect()
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper


def readAll(path, fileType):
    files = os.listdir (path)
    # textContainer = []
    excelFileList = []
    for file in files:
        file = path + '/' + file
        if not os.path.isfile (file):
            continue
        if file.endswith (fileType):
            excelFileList.append (file)
            print (file)

    return excelFileList


# 日期格式: 2017-12-13
def get_time_futureid_v1(fn=None):
    # fn = '交易统计信息_2017-12-11-5年期国债.xml'
    ans = fn.split ('-', 1)
    pre_ans = ans[0]
    tail_ans = ans[1]

    ftime = tail_ans.split ('.')[0]
    ftime = ftime.replace ('-', '')

    return ftime


def get_time_futureid(fn=None):
    # fn = '交易统计信息_2017-12-11-5年期国债.xml'
    ans = fn.rsplit ('-', 1)
    pre_ans = ans[0]
    tail_ans = ans[1]
    pre_split = pre_ans.split ('_')
    tail_split = tail_ans.split ('.')
    ftime = pre_split[1]
    ftime = ftime.replace ('-', '')
    future_id = tail_split[0]
    return (ftime, future_id)

def get_time(fn=None):
    # fn = '日期：2017-12-7'
    ans = fn.rsplit ('：', 1)
    tail_ans = ans[1]
    time = datetime.datetime.strptime (tail_ans, '%Y-%m-%d')
    str = time.strftime ('%Y%m%d')
    int_time = int(str)

    return int_time, time


def get_data(futures_path='xls/futures', file_type='xlsx'):
    fut_files = readAll (futures_path, file_type)
    df_list = []
    for a_file in fut_files:
        # df = pd.read_csv(a_file, header=0, skipfooter=1, encoding='python', encoding='utf8')
        # pandas的索引函数主要有三种：
        # loc 标签索引，行和列的名称
        # iloc 整型索引（绝对位置索引），绝对意义上的几行几列，起始索引为0
        # ix 是 iloc 和 loc的合体
        # at是loc的快捷方式
        # iat是iloc的快捷方式

        # df = pd.read_excel(a_file, header=2, skip_blank_lines=True)
        df = pd.read_excel (a_file, header=None, skip_blank_lines=True)
        print (df.iloc[0])
        print (df.iloc[1])
        print (df.iloc[2])

        time_row = df.iloc[1]
        time_row = time_row.dropna().tolist()
        time_str = None
        for element in time_row:
            if '日期' in element:
                time_str = element
        if not time_row:
            return
        int_time,file_time = get_time(time_str)

        clean_df = df[3:]
        clean_df = clean_df.reset_index(drop=True)
        print (clean_df)
        print(df.iloc[2])
        print (df.iloc[2].tolist ())
        clean_df.columns = df.iloc[2].tolist()
        clean_df.rename(columns=lambda x: x.strip(), inplace=True)
        number_cols = { '投资者代码', '产品份额', '最新净值', '单位净值', '预警线', '清盘线'}
        old_col_names = clean_df.columns.tolist()
        for col in old_col_names:
            if col in number_cols:
                clean_df[col] = clean_df[col].replace(['-', '不设清盘线'], [0, 0])
                # clean_df[col].replace ({'-':0, '不设清盘线':0})
                clean_df[col] = clean_df[col].fillna (0)
            else:
                clean_df[col] = clean_df[col].replace({'-': '0', '不设清盘线': '0'})
                clean_df[col] = clean_df[col].fillna('0')
        # clean_df = clean_df.replace ('-', 0)
        # clean_df = clean_df.fillna (0)
        print (clean_df)

        # 备案号product_code	 投资者代码acc_id	产品名称product_name	产品份额product_share
        # 最新净值manage_asset_value
        # 单位净值current_nav
        # 预警线warning_line	清盘线 Liquidation line	备注remarks

        cols_dict = {'备案号': 'product_code', '投资者代码': 'acc_id', '产品名称': 'product_name',
                     '产品份额': 'product_share', '最新净值': 'manage_asset_value',
                     '单位净值': 'current_nav',
                     '预警线': 'warning_line', '清盘线': 'liquidation_line', '备注': 'remarks'}
        clean_df.rename (columns=lambda x: x.strip (), inplace=True)
        clean_df.rename (columns=cols_dict, inplace=True)
        clean_df['report_date'] = int_time
        # clean_df['id'] = 0
        print(clean_df)
        df_list.append (clean_df)
    return df_list


def insert_db(data, tablename='trades'):
    metadata = MetaData ()
    # engine = create_engine ('sqlite:///trades.sqlite')
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    # engine = create_engine ('sqlite:///test1.sqlite')
    # engine = create_engine ('sqlite:///test1.sqlite')
    source_constr = hsfa_constr
    engine = create_engine (source_constr, encoding='gbk', echo=True)

    connection = engine.connect ()
    # transaction = connection.begin()

    try:

        data.to_sql (tablename.lower (), engine, if_exists='append', index=False)

        # transaction.commit()
        ans = True
    except IntegrityError as error:
        # transaction.rollback()
        print (error)
    except Exception as error:
        # transaction.rollback()
        print (error)
    finally:
        connection.close ()
    return


def insert_direct(data_list=None, tablename='trades'):
    # metadata = MetaData()
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    # engine = create_engine ('sqlite:///test1.sqlite')
    # engine = create_engine ('sqlite:///test1.sqlite')
    # 生成ORM基类
    Base = declarative_base()
    source_constr = hsfa_constr
    engine = create_engine (source_constr, encoding='gbk', echo=True)
    connection = engine.connect ()
    # meta = MetaData(bind=engine, reflect=True)
    # o_table = meta.tables[tablename.lower()]
    meta = MetaData (bind=engine)
    from sqlalchemy import DateTime
    o_table = Table (tablename.lower (), meta,
                     Column('id', Integer, Sequence('id_seq'), primary_key=True),
                     Column('last_modified_date', DateTime, default=datetime.datetime.now),
                     autoload=True)
    # t = Table('mytable', metadata,
    #           Column('id', Integer, Sequence('id_seq'), primary_key=True),
    #           autoload=True
    #           )
    Session = sessionmaker(bind=engine)
    session_obj = Session()

    class newtable_class(object):
        pass

    mapper(newtable_class, o_table)


    # action = o_table.insert().values(data_list[0])

    ans = False
    try:
        print(data_list)
        # newtable_obj_1 = newtable_class()
        # newtable_obj_1.time = datetime(2018,1,2,15,1,00)

        # newtable_obj_2 = newtable_class(data_list)
        # newtable_obj_2.time = datetime(2018,1,2,15,2,00)

        # session_obj.merge( newtable_obj )#有就更新，没有就插入
        # session_obj.merge(newtable_obj_1)
        # session_obj.merge(newtable_obj_2)
        # session_obj.commit()


        ins = o_table.insert ()
        print(data_list)
        ans = connection.execute (ins, data_list)


        ans = True
    except IntegrityError as error:

        print (error)
    except Exception as error:

        print (error)
    finally:
        connection.close ()

    return ans


def pd_insert_db(df):
    data_list = df.to_dict (orient='records')
    # insert_db(data_list, tablename='o32nav')
    insert_direct (data_list, tablename='new_table')

    return


def init_db(path='./insert_db'):
    df_list = get_data (futures_path=path)
    for df in df_list:
        # insert_db(df, tablename='o32nav')
        pd_insert_db (df)
    # df_list = get_data()
    # for df in df_list:
    #     pd_insert_db(df)
    return


def main():
    init_db ()
    # data.to_sql(tablename.lower(), engine, if_exists='append', index=False)
    # df_list = get_data()
    # df = df_list[0]
    # print(df)
    # for df in df_list:
    #     pd_insert_db(df)

    return


if __name__ == '__main__':
    main ()
