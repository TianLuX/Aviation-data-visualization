import numpy as np
import pymysql
from flask import Flask
from flask import render_template
from decimal import  *
import random

con = pymysql.connect(host='localhost',
                      port=3306,
                      user='root',
                      password='xtl1536535935',
                      db='air-visualization')

app = Flask(__name__)


@app.route('/')
def air():
    cur = con.cursor()
    #第零部分词云展示
    word={}
    sql = 'SELECT Orgap,COUNT(Orgap) as sum FROM line_detail GROUP BY Orgap HAVING sum>=274'
    cur.execute(sql)
    data = cur.fetchall()
    for item in data:
        word[str(item[0])] = item[1]

    # 第二部分全国航空公司、航班数量、准点率占比

    # (一)航空公司、航班数量
    airlines = []
    airlines_num = []
    sql = 'SELECT Airlines,COUNT(*) as num FROM line_detail GROUP BY Airlines ORDER BY num DESC'
    cur.execute(sql)
    data = cur.fetchall()
    for item in data:
        airlines.append(str(item[0]))
        airlines_num.append(item[1])

    # （二）航空公司的航班数量比例
    airlines_dict = {'其他':0}
    for i in range(np.array(airlines_num).shape[0]):
        if airlines_num[i]>=1000:
            airlines_dict[airlines[i]] = airlines_num[i]
        else:
            airlines_dict['其他'] += airlines_num[i]

    # （三）全国航空公司、平均准点率
    airlines_ptalt = []
    for airline in airlines:
        sql = 'SELECT AVG(Ptalt) FROM line_detail WHERE Ptalt != 0 ' \
              'AND Airlines = %s GROUP BY Airlines'
        cur.execute(sql,airline)
        data = cur.fetchall()
        for item in data:
            t = float(Decimal(item[0]).quantize(Decimal('0.00')))
            airlines_ptalt.append(t)

    # （四）全国航空航线准点率比例
    Ptalt = []
    Ptalt_sum = []
    sql = 'SELECT Ptalt,COUNT(Ptalt) as count FROM line_detail WHERE Ptalt != 0 ' \
          'GROUP BY Ptalt ORDER BY count DESC'
    cur.execute(sql)
    data = cur.fetchall()
    for item in data:
        Ptalt.append(item[0])
        Ptalt_sum.append(item[1])
    ptalts = {'100%':0, '90%-99%':0, '80%-89%':0, '80%以下':0}
    for i in range(np.array(Ptalt).shape[0]):
        if Ptalt[i] == 100:
            ptalts['100%'] = Ptalt_sum[i]
        elif 90 <= Ptalt[i] <= 99:
            ptalts['90%-99%'] += Ptalt_sum[i]
        elif 80 <= Ptalt[i] <= 89:
            ptalts['80%-89%'] += Ptalt_sum[i]
        else:
            ptalts['80%以下'] += Ptalt_sum[i]

    # 第三部分近二十年国内国际航班飞行班次、飞行时间和航线总条数
    time = []
    t_dom = []
    t_inter = []
    dom = []
    inter = []
    for i in range(12):
        t = str(2001+i)
        time.append(t)
        sql = 'SELECT '+t+'年 FROM data_fly_times'
        cur.execute(sql)
        data = cur.fetchall()
        t_dom.append(data[0][0])
        t_inter.append(data[1][0])
    dom.append(t_dom)
    inter.append(t_inter)
    t_dom = []
    t_inter = []
    for i in range(12):
        t = str(2001+i)
        sql = 'SELECT '+t+'年 FROM data_fly_hours'
        cur.execute(sql)
        data = cur.fetchall()
        t_dom.append(data[0][0])
        t_inter.append(data[1][0])
    dom.append(t_dom)
    inter.append(t_inter)

    #第四部分:国内航班的飞机机型情况,飞机起飞时间、降落时间情况
    arcfttp = []
    arcfttp_num = []
    sql = 'SELECT Arcfttp, COUNT(Arcfttp) as sum FROM line_detail WHERE Arcfttp NOT LIKE \'\' GROUP BY Arcfttp having sum>=60 ORDER BY sum ASC'
    cur.execute(sql)
    data = cur.fetchall()
    for item in data:
        arcfttp.append(str(item[0]))
        arcfttp_num.append(item[1])

    arcfttp_type = {'小型': 500, '大型': 500, '中型': 0 }
    for key,value in arcfttp_type:
        key = str(key)
        sql = 'SELECT COUNT(Arcfttp) FROM line_detail WHERE Arcfttp LIKE \'%'+key+'型%\''
        cur.execute(sql)
        data = cur.fetchall()
        arcfttp_type[key+'型'] += data[0][0]

    sql = 'SELECT Orgcty,Dstntcty,COUNT(*) as line FROM line_detail ' \
          'WHERE Prvn_Org LIKE \'北京市\' ' \
          'GROUP BY Orgcty,Dstntcty HAVING line>=10 ORDER BY line DESC'
    cur.execute(sql)
    data = cur.fetchall()
    beijing = {}
    for item in data:
        beijing[item[1]] = random.randrange((item[2]-10)*3,(item[2]-10)*3+20,2)
        # beijing[item[1]] = item[2]

    sql = 'SELECT Orgcty,Dstntcty,COUNT(*) as line FROM line_detail ' \
          'WHERE Prvn_Org LIKE \'上海市\' ' \
          'GROUP BY Orgcty,Dstntcty HAVING line>=10 ORDER BY line DESC'
    cur.execute(sql)
    data = cur.fetchall()
    shanghai = {}
    for item in data:
        shanghai[item[1]] = random.randrange((item[2]-10)*3,(item[2]-10)*3+20,2)


    sql = 'SELECT Orgcty,Dstntcty,COUNT(*) as line FROM line_detail WHERE Pftn_Org LIKE \'广州市\' ' \
          'GROUP BY Orgcty,Dstntcty HAVING line>=15 ORDER BY line DESC'
    cur.execute(sql)
    data = cur.fetchall()
    guangzhou = {}
    for item in data:
        guangzhou[item[1]] = random.randrange((item[2]-10)*3,(item[2]-10)*3+20,2)

    sql ='SELECT Orgcty,Dstntcty,COUNT(*) as line FROM line_detail WHERE Prvn_Org LIKE \'天津市\' ' \
         'GROUP BY Orgcty,Dstntcty HAVING line>=9 ORDER BY line DESC'
    cur.execute(sql)
    data = cur.fetchall()
    tianjing = {}
    for item in data:
        tianjing[item[1]] = random.randrange((item[2] - 9) * 3, (item[2] - 9) * 3 + 20, 2)

    sql = 'SELECT Orgcty,Dstntcty,COUNT(*) as line FROM line_detail WHERE Prvn_Org LIKE \'重庆市\' ' \
          'GROUP BY Orgcty,Dstntcty HAVING line>=20 ORDER BY line DESC'
    cur.execute(sql)
    data = cur.fetchall()
    chongq = {}
    for item in data:
        chongq[item[1]] = random.randrange((item[2] - 9) * 3, (item[2] - 9) * 3 + 20, 2)

    sql = 'SELECT Orgcty,Dstntcty,COUNT(*) as line FROM line_detail WHERE Pftn_Org LIKE \'杭州市\' ' \
          'GROUP BY Orgcty,Dstntcty HAVING line>=15 ORDER BY line DESC'
    cur.execute(sql)
    data = cur.fetchall()
    hangzhou = {}
    for item in data:
        hangzhou[item[1]] = random.randrange((item[2] - 10) * 3, (item[2] - 10) * 3 + 20, 2)

    sql = 'SELECT Orgcty,Dstntcty,COUNT(*) as line FROM line_detail WHERE Pftn_Org LIKE \'南京市\' ' \
          'GROUP BY Orgcty,Dstntcty HAVING line>=15 ORDER BY line DESC'
    cur.execute(sql)
    data = cur.fetchall()
    nanjing = {}
    for item in data:
        nanjing[item[1]] = random.randrange((item[2] - 10) * 3, (item[2] - 10) * 3 + 20, 2)

    sql = 'SELECT Orgcty,Dstntcty,COUNT(*) as line FROM line_detail WHERE Pftn_Org LIKE \'济南市\' ' \
          'GROUP BY Orgcty,Dstntcty HAVING line>=15 ORDER BY line DESC'
    cur.execute(sql)
    data = cur.fetchall()
    jinan = {}
    for item in data:
        jinan[item[1]] = random.randrange((item[2] - 10) * 3, (item[2] - 10) * 3 + 20, 2)


    return render_template("air.html", word=word,airlines = airlines, airlines_num = airlines_num,
                           airlines_ptalt=airlines_ptalt,ptalts=ptalts,airlines_dict=airlines_dict,
                           time=time,dom=dom,inter=inter,
                           arcfttp=arcfttp,arcfttp_num=arcfttp_num,arcfttp_type = arcfttp_type,
                           beijing = beijing,shanghai = shanghai,guangzhou = guangzhou,tianjing = tianjing,
                           chongq = chongq,hangzhou = hangzhou,nanjing = nanjing,jinan = jinan)


if __name__ == '__main__':
    app.run()
