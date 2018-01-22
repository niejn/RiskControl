import datetime

import time

str = '2012-11-19'

date_time = datetime.datetime.strptime(str,'%Y-%m-%d')

date_time

date_time.strftime('%Y-%m-%d')

time_time = time.mktime(date_time.timetuple())

time.strftime('%Y-%m-%d',time.localtime(time_time))

date = datetime.date.today()

date

datetime.date(2012,11,19)

datetime.datetime.strptime(str(date),'%Y-%m-%d') #将date转换为str，在由str转换为datetime

datetime.datetime(2012,11,19,0,0)