# -*- coding: utf-8 -*-

# 爬取时光网上电影院相关数据
# 暂定只选择北京、上海、天津、重庆、广州、杭州、成都、南京这些城市（热门城市）的数据

import urllib2
import re
import MySQLdb
from bs4 import BeautifulSoup
from xml.dom import minidom  
from xml.dom.minidom import Document


# 网页爬虫，参数是城市和城市对应的后缀
#（如：时光网南京影讯的网址格式是http://theater.mtime.com/China_Jiangsu_Province_Nanjing/，传进来的参数就是Nanjing和Jiangsu_Province_Nanjing）
# 将得到的网页内容保存在了全局变量scriptStrArr中
def spiderCrawl(city, cityPostfix):
    
    print "Now is spidering the webpage"
    
    # 定义代理
    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'}
    # 要爬网页的网址
    visitUrl = 'http://theater.mtime.com/China_'+cityPostfix+"/"
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
 
 
# 对网页内容的解析，将解析出来的有效数据保存到数据库的cinemas表中
def parseContent(city):
 
    print "Now is parsing the content"
        
    # 打开数据库连接
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd=password,db ='mtime_movie' ,charset="utf8")
    # 使用cursor()方法获取操作游标 
    cur = conn.cursor()
    
    # 这里由于cinema的信息较不频繁地改变，所以不清空cinemas表内容，只做更新
     
    for k,v in enumerate(scriptStrArr):
        scriptStr = v
        # print scriptStr
    
        # 匹配所有影院的Json格式正则表达式
        cinemaJsonPattern = re.compile('var cinemasJson = {"totalcount":.*?,"list":(.*?)};', re.S)
        cinemaJsonContent = re.findall(cinemaJsonPattern, scriptStr)
        cinemaJsonContentStr = ''.join(cinemaJsonContent)+"},"
        # print cinemaJsonContentStr
    
        # 匹配单个影院的Json格式正则表达式
        oneCinemaPattern = re.compile('{(.*?)},', re.S)
        oneCinemaContent = re.findall(oneCinemaPattern, cinemaJsonContentStr) # 单个影院的所有内容
        for k,v in enumerate(oneCinemaContent):
            oneCinemaContentStr = ''.join(v)
            # print oneCinemaContentStr
            # print oneCinemaContentStr.__len__()
            oneCinemaIDPattern = re.compile('"cid":(.*?),')
            oneCinemaID = re.findall(oneCinemaIDPattern, oneCinemaContentStr) # 单个影院的编号
            print oneCinemaID[0]
            oneCinemaNamePattern = re.compile('"cname":(.*?),', re.S)
            oneCinemaName = re.findall(oneCinemaNamePattern, oneCinemaContentStr) # 单个影院的名称
            print oneCinemaName[0]
            oneCinemaDetailUrlPattern = re.compile('"showtimepage":(.*?),', re.S);
            oneCinemaDetailUrl = re.findall(oneCinemaDetailUrlPattern, oneCinemaContentStr) # 单个影院的详细信息对应网址
            print oneCinemaDetailUrl[0]
            oneCinemaImgUrlPattern = re.compile('"logo":(.*?),', re.S);
            oneCinemaImgUrl = re.findall(oneCinemaImgUrlPattern, oneCinemaContentStr) # 单个影院的图片对应网址
            print oneCinemaImgUrl[0]
            oneCinemaAddressPattern = re.compile('"address":(.*?),', re.S)
            oneCinemaAddress = re.findall(oneCinemaAddressPattern, oneCinemaContentStr) # 单个影院的地址
            oneCinemaAddressStr = ''.join(oneCinemaAddress[0])
            if oneCinemaAddressStr.__len__() == 0:
                oneCinemaAddressStr = ""
            if not oneCinemaAddressStr.endswith('"'):
                oneCinemaAddressStr += '"'
            print oneCinemaAddressStr
            oneCinemaMinCostPattern = re.compile('"lowestprice":"(.*?)"')
            oneCinemaMinCost =  re.findall(oneCinemaMinCostPattern, oneCinemaContentStr) # 单个影院的最低价
            oneCinemaMinCostStr = '"未知"'
            if len(oneCinemaMinCost) != 0:
                oneCinemaMinCostStr = oneCinemaMinCost[0]
            print oneCinemaMinCostStr
            
            SQLStr = "SELECT count(*) from cinemas WHERE ID="+oneCinemaID[0]+""
            print SQLStr
            cur.execute(SQLStr) 
            result = cur.fetchone()[0]
            if result == 0:  # 数据库中还不存在这个影院的信息，把详细信息加到cinemas表中
                SQLStr = "INSERT INTO cinemas (ID, city, name, address, detailURL, ImgURL, minCost) VALUES("+oneCinemaID[0]+", '"+city+"', "+oneCinemaName[0]+", "+oneCinemaAddressStr+", "+oneCinemaDetailUrl[0]+", "+oneCinemaImgUrl[0]+", "+oneCinemaMinCostStr+")"
                print SQLStr
                cur.execute(SQLStr) 
            else:   # 数据库中已经存在这个影院的信息，更新详细信息到cinemas表中
                SQLStr = "UPDATE cinemas SET address=%s, detailURL=%s, ImgURL=%s, minCost=%s WHERE ID=%s" %(oneCinemaAddressStr, oneCinemaDetailUrl[0], oneCinemaImgUrl[0], oneCinemaMinCostStr, oneCinemaID[0])
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
     
    cinemaList = doc.createElement('cinemaList') # 创建根元素
    doc.appendChild(cinemaList) # 添加根元素

    SQLStr = "SELECT * FROM cinemas"
    cur.execute(SQLStr)
    results = cur.fetchall()
    
    for r in results:
        print r[0], r[1], r[2], r[3], r[5], r[6]
    
        cinema = doc.createElement("cinema")
        cinemaList.appendChild(cinema)
        ID = doc.createElement("ID")
        IDValue = doc.createTextNode(str(r[0]))
        ID.appendChild(IDValue)
        cinema.appendChild(ID)
        city = doc.createElement("city")
        cityValue = doc.createTextNode(r[1])
        city.appendChild(cityValue)
        cinema.appendChild(city)
        name = doc.createElement("name")
        nameValue = doc.createTextNode(r[2])
        name.appendChild(nameValue)
        cinema.appendChild(name)
        address = doc.createElement("address")
        addressValue =doc.createTextNode(r[3])
        address.appendChild(addressValue)
        cinema.appendChild(address)
        imgURL = doc.createElement("imgURL")
        imgURLValue = doc.createTextNode(r[5])
        imgURL.appendChild(imgURLValue)
        cinema.appendChild(imgURL)
        minCost = doc.createElement("minCost")
        minCostValue = doc.createTextNode(str(r[6]))
        minCost.appendChild(minCostValue)
        cinema.appendChild(minCost)

    with open("mtime_cinemaInfo.xml", 'w') as f:
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

    global scriptStrArr
  
    global password
    password = ''
    password = raw_input("Input your MySQL password:")
    print "Your password is: "+password
    
    # 只爬南京的数据
    spiderCrawl("Nanjing","Jiangsu_Province_Nanjing")
    parseContent("Nanjing")
    createTheXML()