# -*- coding: utf-8 -*-

# 爬取时光网上订票相关数据
# 暂定只选择北京、上海、天津、重庆、广州、杭州、成都、南京这些城市（热门城市）的数据
# 时光网上的订票只能够购买当天及后三天的电影票，共4天的数据

import urllib2
import re
import Queue
import MySQLdb
import datetime
from bs4 import BeautifulSoup
from xml.dom import minidom  
from xml.dom.minidom import Document


queue = Queue.Queue(100)  # 队列，大小为100
filmID = 0 # film的ID，在爬虫中使用
eachSize = 100 # 每次从MySQL中取多少条数据
counter = 0 # 已经从MySQL中取的次数


# 从数据库中取出电影编号，每次取num个，从数据库的start位置开始取
# 得到的内容保存到队列queue中
def fetchFilmDataFromSQL(start, num):
    
    # 打开数据库连接
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd=password,db ='mtime_movie')
    # 使用cursor()方法获取操作游标 
    cur = conn.cursor()
    
    # 使用execute方法执行SQL语句
    IDs = cur.execute("SELECT ID from films")
    print IDs
    cur.scroll(start,'absolute')
    IDList = cur.fetchmany(num)
    for ID in IDList:
        id = ID[0]
        queue.put(id)
        
    # 关闭连接
    cur.close()
    conn.commit()
    conn.close()
 
# 数据库的初始化：每次运行前清空buys表
# 之所以不放在parseTheContent方法中，是因为循环中反复执行parseTheContent方法，若放在其中，则只剩下最后一条数据
def initTheSQL():
    
    # 打开数据库连接
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd=password,db ='mtime_movie' ,charset="utf8")
    # 使用cursor()方法获取操作游标 
    cur = conn.cursor()
    
    # 清空buys表内容
    SQLStr = "TRUNCATE TABLE buys"
    cur.execute(SQLStr)   
    
    # 关闭连接
    cur.close()
    conn.commit()
    conn.close()


# 网页爬虫，参数是城市、城市对应的后缀、电影编号和查询日期
#（如：时光网南京市订2017年6月5日“神奇女侠”电影票的网址格式是http://theater.mtime.com/China_Jiangsu_Province_Nanjing/movie/40205/20170605/，传进来的参数就是Nanjing,Jiangsu_Province_Nanjing,40205和20170605）
# 将得到的网页内容保存在了全局变量scriptStrArr中
def spiderCrawl(city, cityPostfix, filmID, searchDate):
    
    print "Now is spidering the webpage"
    
    
    # 定义代理
    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    # 要爬网页的网址
    visitUrl = 'http://theater.mtime.com/China_'+cityPostfix+"/movie/"+filmID+"/"+searchDate+"/" 
    print visitUrl
    
    global scriptStrArr
    scriptStrArr = []
    scriptStr = '' # 这里事先将script定义成空字符串，防止返回404 Not Found时解析函数报错
        
    try:
        request = urllib2.Request(visitUrl, headers=headers)
        response = urllib2.urlopen(request)
        
        soup = BeautifulSoup(response)
        # print soup.prettify()  
        for script in soup('script'):
            # print script 
            scriptStr = ''.join(script)
            # print scriptStr
            scriptStrArr.append(scriptStr)
     
    except urllib2.HTTPError, e:
        if hasattr(e, "code"):
            print e.code
        if hasattr(e, "reason"):
            print e.reason
          
    print "Spidering the webpage ends"
 
    
# 对网页内容的解析，将解析出来的有效数据保存到数据库的costs表中
def parseContent(city, filmID):
    
    print "Now is parsing the content"
     
    # 打开数据库连接
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd=password,db ='mtime_movie' ,charset="utf8")
    # 使用cursor()方法获取操作游标 
    cur = conn.cursor()
    
    dict = {"January":"01", "February":"02", "March":"03", "April":"04", "May":"05", "June":"06", "July":"07", "August":"08", "September":"09", "October":"10", "November":"11", "December":"12"}
    
    for k,v in enumerate(scriptStrArr):
        scriptStr = v
        # print scriptStr
        
        # 匹配所有电影销售的Json格式正则表达式
        costJsonPattern = re.compile('var showtimesJson = (.*?);', re.S)
        costJsonContent = re.findall(costJsonPattern, scriptStr)
        costJsonContentStr = ''.join(costJsonContent)+"},"
        # print costJsonContentStr
         
        # 匹配单个电影销售的Json格式正则表达式
        oneCostPattern = re.compile('{(.*?)},', re.S)
        oneCostContent = re.findall(oneCostPattern, costJsonContentStr) # 单个电影销售的所有内容
        for k,v in enumerate(oneCostContent):
            oneCostContentStr = ''.join(v)
            # print oneCostContentStr
            oneCostIDPattern = re.compile('"showtime_ID":(.*?),', re.S) 
            oneCostID = re.findall(oneCostIDPattern, oneCostContentStr) # （这个可能是）单个电影销售的主键
            # print oneCostID[0]
            SQLStr = "SELECT count(*) FROM buys WHERE buyID="+oneCostID[0]+""
            cur.execute(SQLStr)
            result = cur.fetchone()[0]
            # print result
            if result == 0: # 数据库中还不存在这个电影销售的信息，把详细信息加到cost表中
                oneCostCinemaPattern = re.compile('"cinemaId":(.*?),', re.S)
                oneCostCinema = re.findall(oneCostCinemaPattern, oneCostContentStr) # 单个电影销售的影院编号
                # print oneCostCinema[0]
                oneCostDatePattern = re.compile('"realtime":new Date(.*?),"', re.S);
                oneCostDate = re.findall(oneCostDatePattern, oneCostContentStr) # 单个电影销售的上映日期和时间
                # print oneCostDate[0]
                oneCostDateStr = ''.join(oneCostDate[0])[2:-2] # 截除时间字符串前后的'("")'
                # print oneCostDateStr
                oneCostDateStrSeg = oneCostDateStr.split(' ')
                oneCostMonthStr = oneCostDateStrSeg[0][0:-1] # 截除月份后的','再进行转换
                oneCostMonth = dict.get(oneCostMonthStr)
                # print oneCostMonth
                oneCostDayStr = oneCostDateStrSeg[1]
                if oneCostDayStr.__len__() < 2:
                    oneCostDayStr = '0'+oneCostDayStr
                oneCostYearStr = oneCostDateStrSeg[2]
                oneCostTimeStr = oneCostDateStrSeg[3]
                oneCostDateFormat = oneCostYearStr+'-'+oneCostMonth+'-'+oneCostDayStr
                oneCostTimeFormat = oneCostDateFormat+' '+oneCostTimeStr 
                # print oneCostDateFormat
                # print oneCostTimeFormat 
                oneCostPricePattern = re.compile('"mtimePrice":(.*?),', re.S)
                oneCostPrice = re.findall(oneCostPricePattern, oneCostContentStr) # 单个电影销售的价格
                # print oneCostPrice[0]
                    
                SQLStr = "INSERT INTO buys(buyID, filmID, cinemaID, date, time, cost) VALUES ("+oneCostID[0]+", "+filmID+", "+oneCostCinema[0]+", '"+oneCostDateFormat+"', '"+oneCostTimeFormat+"', "+oneCostPrice[0]+")"
                print SQLStr
                cur.execute(SQLStr)   
               
         
    # 关闭连接
    cur.close()
    conn.commit()
    conn.close() 
    
    print "Parsing the content ends"    
            
                
# 根据数据库的内容来生成xml文件
#（如果xml文件也像数据库那样先添加再更新，会产生很多的空行，暂无解决方法；而且，这样的好处也有防止xml中插入相同数据）
def createTheXML():
    
    print "Now is creating the XML"
    
    # 打开数据库连接
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd=password,db ='mtime_movie' ,charset="utf8")
    # 使用cursor()方法获取操作游标 
    cur = conn.cursor()
    
    doc = Document()  # 创建DOM文档对象
     
    buyInfoList = doc.createElement('buyInfoList') # 创建根元素
    doc.appendChild(buyInfoList) # 添加根元素

    SQLStr = "SELECT films.name, cinemas.name, date, time, cost FROM buys, films, cinemas WHERE buys.filmID=films.ID and buys.cinemaID=cinemas.ID"
    cur.execute(SQLStr)
    results = cur.fetchall()
    
    for r in results:
        print r[0], r[1], r[2], r[3], r[4]
    
        buyInfo = doc.createElement('buyInfo')
        buyInfoList.appendChild(buyInfo)
        filmID = doc.createElement("filmName")
        filmIDValue = doc.createTextNode(str(r[0]))
        filmID.appendChild(filmIDValue)
        buyInfo.appendChild(filmID)
        cinemaID = doc.createElement("cinemaName")
        cinemaIDValue = doc.createTextNode(str(r[1]))
        cinemaID.appendChild(cinemaIDValue)
        buyInfo.appendChild(cinemaID)
        time = doc.createElement("time")
        date = doc.createElement("date")
        dateValue = doc.createTextNode(str(r[2]))
        date.appendChild(dateValue)
        time.appendChild(date)
        detailTime = doc.createElement("detailTime")
        detailTimeValue = doc.createTextNode(str(r[3]))
        detailTime.appendChild(detailTimeValue)
        time.appendChild(detailTime)
        buyInfo.appendChild(time)
        cost = doc.createElement("cost")
        costValue = doc.createTextNode(str(r[4]))
        cost.appendChild(costValue)
        buyInfo.appendChild(cost)
        platform = doc.createElement("platform")
        platformValue = doc.createTextNode("0") # 时光网对应的编号
        platform.appendChild(platformValue)
        buyInfo.appendChild(platform)

    with open("mtime_buyInfo.xml", 'w') as f:
        f.write(doc.toprettyxml(indent='\t', encoding='utf-8'))                 
     
    # 关闭连接
    cur.close()
    conn.commit()
    conn.close()
    
    print "Creating the XML ends"


if __name__ == "__main__": 
#     global cityMap
#
#     cityMap = {"Beijing":"Beijing", "Shanghai":"Shanghai", "Tianjin":"Tianjin", "Chongqing":"Chongqing", 
#         "Guangzhou":"Guangdong_Province_Guangzhou", "Hangzhou":"Zhejiang_Province_Hangzhou", 
#         "Chengdu":"Sichuan_Province_Chengdu", "Nanjing":"Jiangsu_Province_Nanjing"}
    
    global counter, eachSize
    global scriptStrArr

    global password
    password = ''
    password = raw_input("Input your MySQL password:")
    print "Your password is: "+password
    
    initTheSQL()
#     cityName = cityMap.keys();
#     for k, v in enumerate(cityName):
#         cityPostfix = cityMap.get(v)
#         
#         counter = 0;
#         while counter*eachSize < 100: # 这个数字是目前数据库中film表中的记录数
#             if queue.empty():
#                 fetchFilmDataFromSQL(counter*eachSize, eachSize)
#                 counter += 1
#                 
#             while not queue.empty():
#                 filmID = queue.get()
#                
#                 # 获取当前日期
#                 now = datetime.datetime.now()
#                 for i in range(0,4): # 爬取从今天开始4天的数据
#                     delta = datetime.timedelta(days=i)
#                     following = now+delta
#                     thatDay = following.strftime('%Y%m%d')
#                     spiderCrawl(v, cityPostfix, str(filmID), thatDay)
#                     parseContent(v)
    
    counter = 0;
    while counter*eachSize < 100: # 这个数字是目前数据库中film表中的记录数
        if queue.empty():
            fetchFilmDataFromSQL(counter*eachSize, eachSize)
            counter += 1
               
        while not queue.empty():
            filmID = queue.get()
              
            # 获取当前日期
            now = datetime.datetime.now()
            for i in range(0,4): # 爬取从今天开始4天的数据
                delta = datetime.timedelta(days=i)
                following = now+delta
                thatDay = following.strftime('%Y%m%d')
                spiderCrawl("Nanjing", "Jiangsu_Province_Nanjing", str(filmID), thatDay)
                parseContent("Nanjing", str(filmID))
    
    createTheXML()
