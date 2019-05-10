# coding = utf-8
import requests
from lxml import etree
import re,time
import random
from faker import Faker
from random import choice
import threading
from pymongo import MongoClient

client = MongoClient(
	'mongodb://root:Lansi123@dds-uf605bb40eca92541596-pub.mongodb.rds.aliyuncs.com:3717,dds-uf605bb40eca92542332-pub.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-4720883')
my_db = client['anjuke']
my_col = my_db['house_col']


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
	except:
		res = ''
	return res


def MyThread(methodThread):
	
	threads = []
	for i in range(15):
		th = threading.Thread(target=methodThread)
		# 加入线程列表
		threads.append(th)
	# 开始线程
	for i in range(15):
		threads[i].setDaemon(True)
		threads[i].start()
	# 结束线程
	for i in range(15):
		threads[i].join()


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


def parse_house():
	while True:
		item = {}
		query_dict = my_col.find_one({'tag':1})
		url = query_dict['href']
		item['houseid'] = query_dict['houseid']
		while True:
			res = Download_item(url)
			if res:
				if 'houseInfo-detail-item' not in res:
					my_col.update_one({'houseid':"{}".format(item['houseid'])},{"$set":{'tag':0}})
					break
				html = etree.HTML(res)
				try:
					item['village'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[1]/div[2]/a/text()")
					item['village'] = item['village'][0]
					item['house_type'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[2]/div[2]/text()")
					item['house_type'] = re.sub('\r|\n|\t', "", item['house_type'][0])
					item['house_type'] = item['house_type'].strip()
	
					item['unit-price'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[3]/div[2]/text()")
					item['unit-price'] = re.search(r'(\d+)', item['unit-price'][0]).group(1)
					adress = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[4]/div/p//text()")
					adress[3] = re.sub(r'\r|\n|\t', '', adress[3])
					item['adress'] = adress[0].strip() + adress[1].strip() + adress[2].strip() + adress[3].strip()
					item['acreage'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[5]/div[2]/text()")[0]
					item['down_payment'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[6]/div[2]/text()")[0].strip()
					item['years'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[7]/div[2]/text()")[0].strip()
					# 朝向
					item['direction'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[8]/div[2]/text()")[0]
					# 房屋类型
					item['house_nature'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[10]/div[2]/text()")[0]
					item['floor'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[11]/div[2]/text()")[0]
					# 装修
					item['Renovation'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[12]/div[2]/text()")[0]
					# 产权年限
					item['property_right_years'] = html.xpath(
						"//ul[@class='houseInfo-detail-list clearfix']/li[13]/div[2]/text()")[0]
					# 电梯
					item['elevator'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[14]/div[2]/text()")[0]
					# 房本年限
					item['fbnx'] = html.xpath("//ul[@class='houseInfo-detail-list clearfix']/li[15]/div[2]/text()")[0]
					# 产权性质
					item['property_right_nature'] = html.xpath(
						"//ul[@class='houseInfo-detail-list clearfix']/li[16]/div[2]/text()")[0]
					# 是否一手房源
					item['first_hand housing'] = html.xpath(
						"//ul[@class='houseInfo-detail-list clearfix']/li[18]/div[2]/text()")[0]
					print(item)
					my_col.update_one({'houseid': "{}".format(item['houseid'])}, {"$set": {'tag': 2}})
					time.sleep(random.randint(1,3))
					break
				except:
					event.clear()
					time.sleep(random.randint(1, 5))
			else:
				event.clear()
				time.sleep(random.randint(1, 5))
			event.wait()
		if query_dict is None:
			break
			
if __name__ == '__main__':
	num = 0
	event = threading.Event()
	event.set()
	bol = True
	pi = ProxyIP()
	proxyip = pi.GetProxyIp  # 代理IP
	th1 = threading.Thread(target=IsSet)
	th1.start()
	MyThread(parse_house)
	# parse_house()
	print('爬取完成')
	bol = False
	# time.sleep(60*60)