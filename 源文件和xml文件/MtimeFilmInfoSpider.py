# -*- coding: utf-8 -*-

# 爬取时光网上影院相关数据
# 暂定只选择北京、上海、天津、重庆、广州、杭州、成都、南京这些城市（热门城市）的数据

import urllib2
import re
import MySQLdb
import datetime
import time
import xml.dom
from xml.dom import minidom  
from xml.dom.minidom import Document


# 网页爬虫，参数是城市和城市对应的后缀
#（如：时光网南京影讯的网址格式是http://theater.mtime.com/China_Jiangsu_Province_Nanjing/，传进来的参数就是Nanjing和Jiangsu_Province_Nanjing）
# 将得到的网页内容保存在了全局变量allContentCHN和moreData中

def spiderCrawl(city, cityPostfix):
    
    print "Now is spidering the webpage"
    
    # 定义代理
    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    # 要爬网页的网址
    visitUrl = 'http://theater.mtime.com/China_'+cityPostfix+"/" 
    # print visitUrl
      
    try:
         
        request = urllib2.Request(visitUrl, headers=headers)
        response = urllib2.urlopen(request)
         
        global allContentCHN
        allContent = response.read()
        allContentCHN = allContent.encode("utf-8")
        # print allContentCHN
      
    except urllib2.HTTPError, e:
        if hasattr(e, "code"):
            print e.code
        if hasattr(e, "reason"):
            print e.reason
    
    
    # 含有json数据的网址，内容包括评分，场次安排，影片热度等
    now = datetime.datetime.now()
    nowTime = now.strftime('%Y%m%d') # 这里还有个时间参数，不想写了，稍微有一点点麻烦
    anotherUrl = 'http://service.theater.mtime.com/Cinema.api?Ajax_CallBack=true&Ajax_CallBackType=Mtime.Cinema.Services&Ajax_CallBackMethod=CinemaChannelIndexLoadData&Ajax_CrossDomain=1&Ajax_RequestUrl=http%3A%2F%2Ftheater.mtime.com%2F'+cityPostfix+'%2F&t=20176715443758033&Ajax_CallBackArgument0=40205%2C151657&Ajax_CallBackArgument1=&Ajax_CallBackArgument2=40205%2C151657%2C232932%2C236948%2C82423%2C232748%2C231651%2C224894%2C219109%2C244048%2C246544%2C221226%2C232256%2C244238%2C242179%2C218219%2C246602%2C219378%2C232998%2C219638%2C216639%2C237793%2C237670%2C232603%2C233371%2C208054%2C239179%2C234694%2C207927%2C235695%2C244986%2C246614%2C233821%2C246564%2C240417%2C235831%2C225709%2C235475%2C245075%2C246820%2C224149%2C200612%2C234464%2C225093%2C153307%2C215724%2C208911%2C219171%2C217851%2C232271%2C237503%2C237219%2C236875%2C218481&Ajax_CallBackArgument3=247238%2C237067%2C246792%2C225709%2C235475%2C240417%2C207927%2C246614%2C232998%2C235831%2C246564%2C235695%2C244986%2C246820%2C246805%2C246562%2C200612%2C246998%2C234048%2C225724%2C246777%2C235449%2C228478%2C245075%2C234875%2C234464%2C237335%2C228270%2C220307%2C244049%2C240098%2C211901%2C235554%2C237613%2C242162%2C247097%2C230633%2C225821%2C17683%2C225748%2C246993%2C218216%2C236657%2C238986%2C232601%2C244233%2C208828%2C243457%2C226435%2C241726%2C234923%2C237264%2C234547%2C225337%2C242404%2C247008%2C229733%2C230647%2C244030%2C244237%2C230723%2C215122%2C232650%2C228267%2C233631%2C231649%2C246771%2C232877%2C228075'
    print anotherUrl
    
    try:
       
        request = urllib2.Request(anotherUrl, headers=headers)
        response = urllib2.urlopen(request)
        
        global moreData
        moreData = response.read()
        moreData = moreData.encode("utf-8")
        # print moreData
        
    except urllib2.HTTPError, e:
        if hasattr(e, "code"):
            print e.code
        if hasattr(e, "reason"):
            print e.reason    
    
    print "Spidering the webpage ends"
 
 
# 对网页内容的解析，将解析出来的有效数据保存到数据库的films表中
def parseContent(city):
    
    print "Now is parsing the content"
     
    # 打开数据库连接
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd=password,db ='mtime_movie' ,charset="utf8")
    # 使用cursor()方法获取操作游标 
    cur = conn.cursor()
     
#     # 清空films表内容
#     SQLStr = "TRUNCATE TABLE films"
#     cur.execute(SQLStr)
       
    # print allContentCHN
        
    # 匹配第一个影片的正则表达式
    firstOnePattern = re.compile('<div class="firstmovie fl">.*?<dl>.*?<dt>(.*?)<div class="moviebtn">.*?</dd>.*?</dl>.*?</div>', re.S)
    firstOneContent = re.findall(firstOnePattern, allContentCHN) # 第一个影片的所有内容
    firstOneContentStr = ''.join(firstOneContent)
    # print firstOneContentStr
        
    firstOneDetailUrlPattern = re.compile('<h2>.*?<a href=(.*?) target', re.S);
    firstOneDetailUrl = re.findall(firstOneDetailUrlPattern, firstOneContentStr) # 第一个影片的详细信息对应网址
    # print firstOneDetailUrl[0]
    firstOneID = (''.join(firstOneDetailUrl)).split('/')[3] # 第一个影片的编号
    # print firstOneID
        
    SQLStr = "SELECT count(*) FROM films WHERE ID="+firstOneID+""
    # print SQLStr
    cur.execute(SQLStr)
    result = cur.fetchone()[0]
    if result == 0: # 数据库中还不存在这个电影的信息，把详细信息加到film表中
          
        firstOneImgUrlPattern = re.compile('img .*? src=(.*?) alt', re.S);
        firstOneImgUrl = re.findall(firstOneImgUrlPattern, firstOneContentStr) # 第一个影片的图片对应网址
        # print firstOneImgUrl[0]
        firstOneNamePattern = re.compile('<h2>.*?<a href=.*?>(.*?)</a>', re.S)
        firstOneName = re.findall(firstOneNamePattern, firstOneContentStr) # 第一个影片的电影名称
        # print firstOneName[0]
        firstOneTimeSpanPattern = re.compile('<h3.*?>(.*?) -.*?<a href', re.S)
        firstOneTimeSpan = re.findall(firstOneTimeSpanPattern, firstOneContentStr) # 第一个影片的时长
        firstOneTimeSpanStr = '';
        if len(firstOneTimeSpan) > 0:
            firstOneTimeSpanStr =  firstOneTimeSpan[0]
        # print firstOneTimeSpanStr
        firstOneCategoryPattern = re.compile('<a href="http://movie.mtime.com/movie/search/section/.*?>(.*?)</a>')
        firstOneCategory =  re.findall(firstOneCategoryPattern, firstOneContentStr) # 第一个影片的分类
        firstOneCategoryStr = firstOneCategory[0]
        if len(firstOneCategory) > 1:
            firstOneCategoryStr += "/"+firstOneCategory[1]
        # print  firstOneCategoryStr
               
        SQLStr = "INSERT INTO films(ID, name, detailURL, imgURL, category, timeSpan) VALUES ("+firstOneID+", '"+firstOneName[0]+"', "+firstOneDetailUrl[0]+", "+firstOneImgUrl[0]+", '"+firstOneCategoryStr+"', '"+firstOneTimeSpanStr+"')"
        # print SQLStr
        # cur.execute(SQLStr)
           
           
    # 匹配其余热门影片的正则表达式
    otherHotPattern = re.compile('<li class="clearfix">(.*?)</li>', re.S)
    otherHotContent = re.findall(otherHotPattern, allContentCHN) # 其余热门影片的所有内容
    for k,v in enumerate(otherHotContent):
        otherHotOneContentStr = ''.join(v)
        # print otherHotOneContentStr
        otherHotOneDetailUrlPattern = re.compile('<dt>.*?<a href=(.*?) class', re.S);
        otherHotOneDetailUrl = re.findall(otherHotOneDetailUrlPattern, otherHotOneContentStr) # 一个其余热门影片的详细信息对应网址
        # print otherHotOneDetailUrl[0]
        otherHotOneID = (''.join(otherHotOneDetailUrl)).split('/')[3] # 一个其余热门影片的编号
        # print otherHotOneID
            
        SQLStr = "SELECT count(*) FROM films WHERE ID="+otherHotOneID+""
        cur.execute(SQLStr)
        result = cur.fetchone()[0]
        if result == 0: # 数据库中还不存在这个电影的信息，把详细信息加到film表中
           
            otherHotOneImgUrlPattern = re.compile('<img.*?src=(.*?) alt', re.S);
            otherHotOneImgUrl = re.findall(otherHotOneImgUrlPattern, otherHotOneContentStr) # 一个其余热门影片的图片对应网址
            # print otherHotOneImgUrl[0]
            otherHotOneNamePattern = re.compile('<dt>.*?<a href=.*?>(.*?)</a>', re.S)
            otherHotOneName = re.findall(otherHotOneNamePattern, otherHotOneContentStr) # 一个其余热门影片的电影名称
            # print otherHotOneName[0]
            otherHotOneTimeSpanPattern = re.compile('<dd class=.*?>(.*?) -.*?<a href', re.S)
            otherHotOneTimeSpan = re.findall(otherHotOneTimeSpanPattern, otherHotOneContentStr) # 一个其余热门影片的时长
            otherHotOneTimeSpanStr = '';
            if len(otherHotOneTimeSpan) > 0:
                otherHotOneTimeSpanStr =  otherHotOneTimeSpan[0]
            # print otherHotOneTimeSpanStr
            otherHotOneCategoryPattern = re.compile('<a href="http://movie.mtime.com/movie/search/section/.*?>(.*?)</a>')
            otherHotOneCategory =  re.findall(otherHotOneCategoryPattern, otherHotOneContentStr) # 一个其余热门影片的分类
            otherHotOneCategoryStr = ''
            if len(otherHotOneCategory) > 0:
                otherHotOneCategoryStr += otherHotOneCategory[0]
            if len(otherHotOneCategory) > 1:
                otherHotOneCategoryStr += "/"+otherHotOneCategory[1]
            # print  otherHotOneCategoryStr
                      
            SQLStr = "INSERT INTO films(ID, name, detailURL, imgURL, category, timeSpan) VALUES ("+otherHotOneID+", '"+otherHotOneName[0]+"', "+otherHotOneDetailUrl[0]+", "+otherHotOneImgUrl[0]+", '"+otherHotOneCategoryStr+"', '"+otherHotOneTimeSpanStr+"')"
            # print SQLStr
            # cur.execute(SQLStr)
               
    # print moreData
       
    # 匹配影片评分的正则表达式
    hotRatingPattern = re.compile('hotplayRatingList":(.*?),"upcomingTicketList', re.S)
    hotRatingContent = re.findall(hotRatingPattern, moreData) # 影片评分数据的所有内容
    hotRatingContentStr = ''.join(hotRatingContent);
    # print hotRatingContentStr
           
    oneRatingPattern = re.compile('{(.*?)}', re.S)
    oneRatingContent = re.findall(oneRatingPattern, hotRatingContentStr)
    for k,v in enumerate(oneRatingContent):
        oneRatingContentStr = ''.join(v)
        # print oneRatingContentStr
        idStr = oneRatingContentStr.split(",")[0]
        id = ''.join(idStr).split(":")[1]
        ratingStr = oneRatingContentStr.split(",")[1] 
        rating = ''.join(ratingStr).split(":")[1]
        SQLStr = "UPDATE films SET score=%s where ID = %s" % (rating, id)
        # print SQLStr
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
     
    filmList = doc.createElement('filmList') # 创建根元素
    doc.appendChild(filmList) # 添加根元素

    SQLStr = "SELECT * FROM films"
    cur.execute(SQLStr)
    results = cur.fetchall()
    
    for r in results:
        print r[0], r[1], r[3], r[4], r[5], r[6]
    
        film = doc.createElement('film')
        filmList.appendChild(film)
        ID = doc.createElement("ID")
        IDValue = doc.createTextNode(str(r[0]))
        ID.appendChild(IDValue)
        film.appendChild(ID)
        name = doc.createElement("name")
        nameValue = doc.createTextNode(r[1])
        name.appendChild(nameValue)
        film.appendChild(name)
        imgURL = doc.createElement("imgURL")
        imgURLValue = doc.createTextNode(r[3])
        imgURL.appendChild(imgURLValue)
        film.appendChild(imgURL)
        category = doc.createElement("category")
        categoryValue = doc.createTextNode(r[4])
        category.appendChild(categoryValue)
        film.appendChild(category)
        timeSpan = doc.createElement("timeSpan")
        timeSpanValue =doc.createTextNode(r[5])
        timeSpan.appendChild(timeSpanValue)
        film.appendChild(timeSpan)
        score = doc.createElement("score")
        scoreValue = doc.createTextNode(str(r[6]))
        score.appendChild(scoreValue)
        film.appendChild(score)
        platform = doc.createElement("platform")
        platformValue = doc.createTextNode("0") # 时光网对应的编号
        platform.appendChild(platformValue)
        film.appendChild(platform)
        
    with open("mtime_filmInfo.xml", 'w') as f:
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
# 
#     cityName = cityMap.keys();
#     for k, v in enumerate(cityName):
#         cityPostfix = cityMap.get(v)
#         spiderCrawl(v, cityPostfix)
#         parseContent(v)
      
    global password
    password = ''
    password = raw_input("Input your MySQL password:")
    print "Your password is: "+password
    
    # 只爬南京的数据
    spiderCrawl("Nanjing","Jiangsu_Province_Nanjing")
    parseContent("Nanjing")
    createTheXML()