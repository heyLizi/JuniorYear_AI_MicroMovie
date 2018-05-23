# -*- coding: utf-8 -*-

# 爬取淘票票上电影相关数据
# 暂定只选择北京、上海、天津、重庆、广州、杭州、成都、南京这些城市（热门城市）的数据

import urllib2
import re
import MySQLdb
import datetime
import time
from selenium import webdriver
from xml.dom import minidom  
from xml.dom.minidom import Document


# 网页爬虫，参数是城市和城市对应的后缀
# 将得到的网页内容保存在了全局变量allContentCHN中

def spiderCrawl(city, pageNum):
    
    print "Now is spidering the webpage"
    
    # 定义代理
    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    # 要爬网页的网址（暂时没看出来什么地点上有什么规律）
    visitUrl = "https://dianying.taobao.com/cinemaList.htm?spm=a1z21.6646273.header.5.AOItDO&n_s=new"
    # 上面那个网址只能显示部分影院，换用下面这个，通过修改页数参数即可
    anotherUrl = "https://dianying.taobao.com/ajaxCinemaList?page="+str(pageNum)+"&amp;regionName=&amp;cinemaName=&amp;pageSize=10&amp;pageLength=8&amp;sortType=0&amp;"
    print anotherUrl
  
    try:
          
        request = urllib2.Request(anotherUrl, headers=headers)
        response = urllib2.urlopen(request)
        
        # print response
        allContent = response.read()
        # print allContent
        global allContentCHN
        allContentCHN = allContent
       
    except urllib2.HTTPError, e:
        if hasattr(e, "code"):
            print e.code
        if hasattr(e, "reason"):
            print e.reason
    
    
#     driver = webdriver.PhantomJS(executable_path=r'F:\Anaconda\Scripts\phantomjs')
#     driver.get(visitUrl)
#     
#     elem = driver.find_element_by_xpath("//li[1]")
#     print elem.text
    
    print "Now is spidering the webpage"
    
 
# 对网页内容的解析，将解析出来的有效数据保存到数据库的cinemas表中
def parseContent(city):
    
    print "Now is parsing the content"
     
    # 打开数据库连接
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd=password,db ='taopiaopiao_movie' ,charset="utf8")
    # 使用cursor()方法获取操作游标 
    cur = conn.cursor()
    
    print allContentCHN
        
    # 匹配全部影院的正则表达式
    cinemaListContentStr = allContentCHN # 全部影院的所有内容
    
    oneCinemaPattern = re.compile('<li.*?>(.*?)</li>', re.S);
    oneCinemaContent = re.findall(oneCinemaPattern, cinemaListContentStr)
    print oneCinemaContent

    for k,v in enumerate(oneCinemaContent):
        oneCinemaContentStr = ''.join(v)
        print oneCinemaContentStr           # 一个影院的全部信息
        
        oneCinemaDetailUrlPattern = re.compile('<h4>.*?<a href=(.*?)>', re.S);
        oneCinemaDetailUrl = re.findall(oneCinemaDetailUrlPattern, oneCinemaContentStr) # 一个影院的详细信息对应网址
        print oneCinemaDetailUrl[0]
        oneCinemaDetailStr = ''.join(oneCinemaDetailUrl)
        oneCinemaIDPattern = re.compile("cinemaId=(.*?)&", re.S)
        oneCinemaID = re.findall(oneCinemaIDPattern, oneCinemaDetailStr) # 一个影院的编号
        print oneCinemaID[0]
        oneCinemaImgUrlPattern = re.compile('<img src=(.*?) alt', re.S);
        oneCinemaImgUrl = re.findall(oneCinemaImgUrlPattern, oneCinemaContentStr) # 一个影院的图片对应网址
        oneCinemaImgUrlStr = ''
        if len(oneCinemaImgUrl) > 0:
            oneCinemaImgUrlStr += oneCinemaImgUrl[0]
        print oneCinemaImgUrlStr
        oneCinemaNamePattern = re.compile('<h4>.*?<a href=.*?>(.*?)</a>', re.S)
        oneCinemaName = re.findall(oneCinemaNamePattern, oneCinemaContentStr) # 一个影院的名称
        print oneCinemaName[0]  
        oneCinemaAddressPattern = re.compile('<span class="limit-address">(.*?)</span>', re.S)
        oneCinemaAddress = re.findall(oneCinemaAddressPattern, oneCinemaContentStr) # 一个影院的地址
        print oneCinemaAddress[0]
        oneCinemaTELPattern = re.compile('<i>电话：</i>(.*?)</div>', re.S)
        oneCinemaTEL = re.findall(oneCinemaTELPattern, oneCinemaContentStr) # 一个影院的电话
        print oneCinemaTEL[0]
        
        SQLStr = "SELECT count(*) FROM cinemas WHERE name='"+oneCinemaName[0]+"'"
        print SQLStr
        cur.execute(SQLStr)
        result = cur.fetchone()[0]
        if result == 0: 
            SQLStr = "INSERT INTO cinemas(ID, city, name, detailURL, imgURL, address, TEL) VALUES ("+oneCinemaID[0]+", '"+city+"', '"+oneCinemaName[0]+"', "+oneCinemaDetailUrl[0]+", '"+oneCinemaImgUrlStr+"', '"+oneCinemaAddress[0]+"', '"+oneCinemaTEL[0]+"')"
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
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd=password,db ='taopiaopiao_movie' ,charset="utf8")
    # 使用cursor()方法获取操作游标 
    cur = conn.cursor()
    
    doc = Document()  # 创建DOM文档对象
     
    cinemaList = doc.createElement('cinemaList') # 创建根元素
    doc.appendChild(cinemaList) # 添加根元素

    SQLStr = "SELECT * FROM cinemas"
    cur.execute(SQLStr)
    results = cur.fetchall()
    
    for r in results:
        print r[0], r[1], r[2], r[4], r[5], r[6]
    
        cinema = doc.createElement('cinema')
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
        addressValue =doc.createTextNode(r[5])
        address.appendChild(addressValue)
        cinema.appendChild(address)
        imgURL = doc.createElement("imgURL")
        imgURLValue = doc.createTextNode(r[4])
        imgURL.appendChild(imgURLValue)
        cinema.appendChild(imgURL)
        TEL = doc.createElement("TEL")
        TELValue = doc.createTextNode(str(r[6]))
        TEL.appendChild(TELValue)
        cinema.appendChild(TEL)

    with open("tpp_cinemaInfo.xml", 'w') as f:
        f.write(doc.toprettyxml(indent='\t', encoding='utf-8')) 
    
    print "Creating the XML ends"

    
if __name__ == "__main__": 
  
    global password
    password = ''
    password = raw_input("Input your MySQL password:")
    print "Your password is: "+password
    
    # 只爬南京的数据
    for i in range(1,9):
        spiderCrawl("Nanjing", i)
        parseContent("Nanjing")
    
    createTheXML()

    