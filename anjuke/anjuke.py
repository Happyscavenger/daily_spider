# coding=utf-8
import requests, time
from faker import Faker
from requests.cookies import RequestsCookieJar
from random import choice
from lxml import etree
import re, threading
from queue import Queue
from copy import deepcopy
import random
from pymongo import MongoClient

# client = MongoClient('mongodb://192.168.0.147:27017/')
client = MongoClient(
	'mongodb://root:Lansi123@dds-uf605bb40eca92541596-pub.mongodb.rds.aliyuncs.com:3717,dds-uf605bb40eca92542332-pub.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-4720883')
my_db = client['anjuke']
my_col = my_db['house_col']

def Download(url):
	f = Faker()
	agents = [f.chrome(), f.firefox(), f.opera(), f.safari()]
	agent = choice(agents)
	headers = {'User-Agent': agent}
	proxies = {'http': 'http://' + proxyip}
	try:
		# cookies = requests.cookies.RequestsCookieJar()
		# cookies=cookies,
		response = requests.get(url, headers=headers, proxies=proxies,  timeout=30)
		# cookies.update(response.cookies)
		res = response.content.decode('utf-8')
		if response.status_code != 200:
			res = ''
	except:
		res = ''
	return res


def Download_item(url):
	f = Faker()
	agents = [f.chrome(), f.firefox(), f.opera(), f.safari()]
	agent = choice(agents)
	headers = {'User-Agent': agent}
	proxies = {'http': 'http://' + proxyip}
	now_time = int(time.time())
	url = url + "&now_time={}".format(now_time)
	try:
		response = requests.get(url, headers=headers, proxies=proxies, timeout=30)
		res = response.content.decode('utf-8')
		if response.status_code != 200:
			res = ''
	except:
		res = ''
	return res


def parse_area():
	url = "https://shanghai.anjuke.com/sale/"
	while True:
		res = Download(url)
		if res:
			html = etree.HTML(res)
			area_list = html.xpath("//div[@class='div-border items-list']/div[1]/span[2]/a")
			item = {}
			for area in area_list[:-1]:
				item['area_href'] = area.xpath('./@href')[0]
				item['area_name'] = area.xpath('./text()')[0]
				area_queue.put(deepcopy(item))
				print(item)
			break
		else:
			event.clear()
			time.sleep(random.randint(1, 5))
		event.wait()


def parse_area_detail():
	acreage_list = ["a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a11"]
	while area_queue.qsize():
		item = area_queue.get()
		url = item['area_href']
		while True:
			res = Download(url)
			if res:
				html = etree.HTML(res)
				a_list = html.xpath("//div[@class='sub-items']/a")
				for a in a_list:
					palte_href = a.xpath('./@href')[0]
					item['plate'] = a.xpath("./text()")[0]
					for acreage in acreage_list:
						item['area_detail_href'] = palte_href + acreage
						plate_queue.put(deepcopy(item))
						print(item)
				break
			else:
				event.clear()
				time.sleep(random.randint(1, 5))
			event.wait()


def parse_house_list():
	item = plate_queue.get()
	url = item['area_detail_href']
	while True:
		res = Download(url)
		if res:
			html = etree.HTML(res)
			try:
				next_url = html.xpath("//a[@class='aNxt']/@href")[0]
				url = next_url
				item['html'] = html
				print('翻页的url是----------------{}'.format(url))
				html_queue.put(deepcopy(item))
			except:
				break
		else:
			event.clear()
			time.sleep(random.randint(1, 5))
		event.wait()


def parse_html():
	item = html_queue.get()
	html = item['html']
	url_list = html.xpath(
		"//ul[@id='houselist-mod-new']/li/div[@class='house-details']/div[@class='house-title']")
	for ul in url_list:
		info={}
		info['href'] = ul.xpath('./a/@href')[0]
		info['href'] = re.search('(.*?)&now_time', info['href']).group(1)
		info['title'] = ul.xpath('./a/@title')[0]
		info['houseid'] = re.search('view/(.*?)\?', info['href']).group(1)
		# house_queue.put(deepcopy(item))
		query_num = my_col.find_one({'houseid': '{}'.format(info['houseid'])})
		if query_num is None:
			print(info)
			my_col.insert_one(info)
			
# def parse_house():
# 	print(house_queue.qsize())
# 	item = house_queue.get()
# 	url = item['href']
# 	while True:
# 		res = Download_item(url)
# 		if res:
# 			# if not 'houseInfo-content' in res:
# 			# 	time.sleep(2)
# 			# 	event.clear()
# 			# event.wait()
# 			if not 'houseInfo-content' in res:
# 				break
# 			html = etree.HTML(res)
# 			item['village'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[1]/div[2]/a/text()")
# 			item['village'] = item['village'][0] if item['village'] else ""
# 			item['house_type'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[2]/div[2]/text()")
# 			if item['house_type']:
# 				item['house_type'] = re.sub('\r|\n|\t', "", item['house_type'][0])
# 				item['house_type'] = item['house_type'].strip()
# 			else:
# 				item['house_type'] = ""
# 			item['unit-price'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[3]/div[2]/text()")
# 			item['unit-price'] = re.search(r'(\d+)', item['unit-price'][0]).group(1) if item['unit-price'] else ""
# 			adress = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[4]/div/p//text()")
# 			if adress:
# 				adress[3] = re.sub(r'\r|\n|\t', '', adress[3])
# 				item['adress'] = adress[0].strip() + adress[1].strip() + adress[2].strip() + adress[3].strip()
# 			else:
# 				item['adress'] = ""
# 			item['acreage'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[5]/div[2]/text()")
# 			item['acreage'] = item['acreage'][0] if item['acreage'] else ""
# 			item['down_payment'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[6]/div[2]/text()")
# 			item['down_payment'] = item['down_payment'][0].strip() if item['down_payment'] else ""
# 			item['years'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[7]/div[2]/text()")
# 			item['years'] = item['years'][0].strip()
# 			# 朝向
# 			item['direction'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[8]/div[2]/text()")
# 			item['direction'] = item['direction'][0] if item['direction'] else ""
# 			# 房屋类型
# 			item['house_nature'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[10]/div[2]/text()")
# 			item['house_nature'] = item['house_nature'][0] if item['house_nature'] else ""
# 			item['floor'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[11]/div[2]/text()")
# 			item['floor'] = item['floor'][0] if item['floor'] else ""
# 			# 装修
# 			item['Renovation'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[12]/div[2]/text()")
# 			item['Renovation'] = item['Renovation'][0] if item['Renovation'] else ""
# 			# 产权年限
# 			item['property_right_years'] = html.xpath(
# 				"//ul[@class='houseInfo-detail-list clearfix']/li[13]/div[2]/text()")
# 			item['property_right_years'] = item['property_right_years'][0] if item['property_right_years'] else ""
# 			# 电梯
# 			item['elevator'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[14]/div[2]/text()")
# 			item['elevator'] = item['elevator'][0] if item['elevator'] else ""
# 			# 房本年限
# 			item['fbnx'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[15]/div[2]/text()")
# 			item['fbnx'] = item['fbnx'][0] if item['fbnx'] else ""
# 			# 产权性质
# 			item['property_right_nature'] = html.xpath(
# 				"//ul[@class='houseInfo-detail-list clearfix']/li[16]/div[2]/text()")
# 			item['property_right_nature'] = item['property_right_nature'][0] if item['property_right_nature'] else ""
# 			# 是否一手房源
# 			item['first_hand housing'] = html.xpath(
# 				"//ul[@class='houseInfo-detail-list clearfix']/li[18]/div[2]/text()")
# 			item['first_hand housing'] = item['first_hand housing'][0] if item['first_hand housing'] else ""
# 			print(item)
# 			time.sleep(random.randint(1, 3))
# 			break
#
# 		else:
# 			event.clear()
# 			time.sleep(random.randint(1, 5))
# 		event.wait()


def MyThread(myqueue, methodThread):
	num = 15
	while myqueue.qsize() > 0:
		time.sleep(random.randint(1,2))
		if methodThread == "":
			break
		if myqueue.qsize() < 15:
			num = myqueue.qsize()
		threads = []
		for i in range(num):
			th = threading.Thread(target=methodThread)
			# 加入线程列表
			threads.append(th)
		# 开始线程
		for i in range(num):
			threads[i].setDaemon(True)
			threads[i].start()
		# 结束线程
		for i in range(num):
			threads[i].join()


def IsSet():
	''' 线程运行状态 '''
	global num
	while True:
		global proxyip
		# 线程阻塞，更换IP
		if not event.isSet():
			try:
				proxyip = pi.GetProxyIp
			except:
				time.sleep(3)
				print('proxyip出错！！！')
				continue
			num += 1
			print('IP更换次数', num)
			print('更换的IP是{}'.format(proxyip))
			# 标记状态为True
			event.set()
		
		time.sleep(5)
		
		if not bol:
			break


class ProxyIP:
	''' 代理IP类 '''
	
	def __init__(self):
		self.order = 'ef8e072865d9e1afdd085731eb44f5d6'
		self.apiUrl = 'http://api.ip.data5u.com/dynamic/get.html?order=' + self.order
		self.res = ''
	
	@property
	def GetProxyIp(self):
		''' 获取代理IP '''
		self.res = requests.get(self.apiUrl).text.strip('\n')
		return self.res


if __name__ == '__main__':
	num = 0
	area_queue = Queue()
	plate_queue = Queue()
	html_queue = Queue()
	# house_queue = Queue()
	event = threading.Event()
	event.set()
	bol = True
	pi = ProxyIP()
	proxyip = pi.GetProxyIp  # 代理IP
	th1 = threading.Thread(target=IsSet)
	th1.start()
	parse_area()
	parse_area_detail()
	MyThread(plate_queue, parse_house_list)
	MyThread(html_queue, parse_html)
	# MyThread(house_queue, parse_house)
	print('爬取完成')
	bol = False
