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


def main():
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

    path = './insert_db'
    df_list = get_data(futures_path=path)
    for df in df_list:
        data_list = df.to_dict(orient='records')
        session.bulk_insert_mappings(O32, data_list)
        session.commit()


if __name__ == '__main__':
    main()
