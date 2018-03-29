import pandas as pd
import xlrd
xlsfile = r'2017-11-21 WorldPeace_V3.5.xlsm'
book = xlrd.open_workbook(xlsfile)
#xlrd用于获取每个sheet的sheetname
#count = len(book.sheets())
with pd.ExcelWriter('2017-11-21 WorldPeace_V3.5.xlsm') as writer:
    for sheet in book.sheets():
        print(sheet.name)
        df = pd.read_excel(xlsfile,sheet.name,index_col = None,na_values= ['9999'])
        df.to_excel(writer,sheet_name = sheet.name)