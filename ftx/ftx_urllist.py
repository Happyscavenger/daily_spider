import requests, queue, threading
from bs4 import BeautifulSoup
from pymongo import MongoClient
import random
from copy import deepcopy
from lxml import etree
import re, time


class FangTxProject(object):
	
	def __init__(self):
		client = MongoClient(
			'xxxx')
		db = client.caiji
		self.ftx = db.fangtianxia
		self.areaList = ['a025', 'a018', 'a019', 'a030', 'a028', 'a020', 'a026', 'a0586', 'a029', 'a023', 'a027',
		                 'a021', 'a024',
		                 'a022', 'a031', 'a032', 'a035', 'a0996']
		
		self.acreageList = ['j20-k250', 'j250-k270', 'j270-k290', 'j290-k2110', 'j2110-k2130', 'j2130-k2150',
		                    'j2150-k2200',
		                    'j2200-k2300', 'j2300-k20']
		self.area_url_queue = queue.Queue()
		self.plate_url_queue = queue.Queue()
		self.house_url_queue = queue.Queue()
		self.event = threading.Event()
		self.order = 'ef8e072865d9e1afdd085731eb44f5d6'
		self.apiUrl = 'http://api.ip.data5u.com/dynamic/get.html?order=' + self.order
		self.proxyip = self.GetProxyIp
		self.num = 0
		self.bol = True
	# 返回代理ip
	@property
	def GetProxyIp(self):
		''' 获取代理IP '''
		self.res = requests.get(self.apiUrl).text.strip('\n')
		return self.res
	# 请求方法
	# def Download(self, url):
	# 	''' 页面下载 '''
	# 	proxies = {'http': 'http://' + self.proxyip}
	# 	first_num = random.randint(55, 62)
	# 	third_num = random.randint(0, 3200)
	# 	fourth_num = random.randint(0, 140)
	# 	os_type = [
	# 		'(Windows NT 6.1; WOW64)', '(Windows NT 10.0; WOW64)', '(X11; Linux x86_64)',
	# 		'(Macintosh; Intel Mac OS X 10_12_6)'
	# 	]
	# 	chrome_version = 'Chrome/{}.0.{}.{}'.format(first_num, third_num, fourth_num)
	#
	# 	ua = ' '.join(['Mozilla/5.0', random.choice(os_type), 'AppleWebKit/537.36',
	# 	               '(KHTML, like Gecko)', chrome_version, 'Safari/537.36']
	# 	              )
	# 	# "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36"
	# 	headers = {
	# 		"user-agent": ua,
	# 		'accept': 'text / html, application / xhtml + xml, application / xml;q = 0.9, image / webp, image / apng, * / *;q = 0.8',
	# 		'accept - encoding': 'gzip, deflate, br',
	# 		'accept - language': 'zh - CN, zh;q = 0.9',
	# 		'cache - control': 'no - cache'
	# 	}
	# 	try:
	# 		response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
	# 		if response.status_code == 200:
	# 			try:
	# 				res = response.content.decode('gbk', 'ignore')
	# 			except:
	# 				res = response.content.decode('gb2312', 'ignore').encode('gb2312')
	# 		else:
	# 			res = ""
	# 	except:
	# 		res = ""
	# 	return res
	
	def Download(self, url):
		''' 页面下载 '''
		# proxies = {'http': 'http://' + self.proxyip, "https": "https://" + self.proxyip}
		first_num = random.randint(55, 62)
		third_num = random.randint(0, 3200)
		fourth_num = random.randint(0, 140)
		os_type = [
			'(Windows NT 6.1; WOW64)', '(Windows NT 10.0; WOW64)', '(X11; Linux x86_64)',
			'(Macintosh; Intel Mac OS X 10_12_6)'
		]
		chrome_version = 'Chrome/{}.0.{}.{}'.format(first_num, third_num, fourth_num)
		
		ua = ' '.join(['Mozilla/5.0', random.choice(os_type), 'AppleWebKit/537.36',
		               '(KHTML, like Gecko)', chrome_version, 'Safari/537.36']
		              )
		headers = {
			"User-Agent": ua,
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Encoding": "gzip, deflate, br",
			"Accept-Language": "zh-CN,zh;q=0.9",
			"Cache-Control": "no-cache",
			"Host": "sh.esf.fang.com",
			"Pragma": "no-cache",
			"Referer": "https://sh.esf.fang.com/",
			"Upgrade-Insecure-Requests": "1"
		}
		try:
			response = requests.get(url, headers=headers,timeout=30)
			if response.status_code == 200:
				res = response.text
			else:
				res = ""
		except:
			res = ""
		return res
	
	# 构造区域url
	def parse_area(self):
		for i in range(0, len(self.areaList)):
			area_url = 'http://esf.sh.fang.com/house-' + self.areaList[i]
			self.area_url_queue.put(deepcopy(area_url))
			print("plate_url is %s" % area_url)
	# 获取板块url
	def parse_plate(self):
		while self.area_url_queue.qsize():
			area_url = self.area_url_queue.get()
			res = self.Download(area_url)
			if res:
				html = etree.HTML(res)
				li_list = html.xpath("//li[@class='area_sq']/ul/li")
				for li in li_list:
					plate_url = li.xpath(".//a/@href")[0]
					for a in range(0, len(self.acreageList)):
						url = deepcopy("https://sh.esf.fang.com" + plate_url + self.acreageList[a])
						print("url is %s" % url)
						self.plate_url_queue.put(deepcopy(url))
						print(self.plate_url_queue.qsize())
			else:
				self.event.clear()
				time.sleep(1)
			self.event.wait()
	# 获取分页信息
	def parse_page(self):
		while self.plate_url_queue.qsize():
			url = self.plate_url_queue.get()
			res = self.Download(url)
			if res:
				total_page_num = re.findall(r"<p>共(\d+)页</p>", res)
				if total_page_num:
					total_page_num = int(total_page_num[0])
					for i in range(1, total_page_num + 1):
						detail_url = url + '-i3' + str(i)
						print("detail_url is %s" % detail_url)
						self.house_url_queue.put(deepcopy(detail_url))
				else:
					self.house_url_queue.put(url)
			else:
				self.event.clear()
				time.sleep(1)
			self.event.wait()
	# 将采集的url存入mongo
	def save_url(self):
		page_url = self.house_url_queue.get()
		res = self.Download(page_url)
		if res:
			html = etree.HTML(res)
			if not '<h4 class="clearfix">' in res:
				return
			dl_list = html.xpath("//div[@class='shop_list shop_list_4']/dl")
			for dl in dl_list:
				house_url = dl.xpath("./dd/h4/a/@href")
				if house_url:
					house_url = house_url[0]
					print(house_url)
					if self.ftx.count_documents({'url': house_url}) <= 0:
						self.ftx.insert({'url': house_url, 'tag': ''})
		else:
			self.event.clear()
			time.sleep(1)
		self.event.wait()
	# 捕获阻塞并更换ip
	def IsSet(self):
		''' 线程运行状态 '''
		while True:
			# 线程阻塞，更换IP
			if not self.event.isSet():
				try:
					self.proxyip = self.GetProxyIp
				except:
					time.sleep(3)
					print('proxyip出错！！！')
					continue
				self.num += 1
				print('IP更换次数', self.num)
				# 标记状态为True
				self.event.set()
			time.sleep(5)
			
			if not self.bol:
				break
	
	# 多线程（队列，方法）
	def MyThread(self, myqueue, methodThread):
		tag = 0
		while myqueue.qsize():
			tag += 1
			print(tag)
			if methodThread == "":
				break
			num = 5
			if myqueue.qsize() < num:
				num = myqueue.qsize()
			
			threads = []
			for i in range(num):
				th = threading.Thread(target=methodThread)
				# 加入线程列表
				threads.append(th)
			
			# 开始线程
			for i in range(num):
				threads[i].start()
			
			# 结束线程
			for i in range(num):
				threads[i].join()
	#　运行程序
	def run(self):
		self.event.set()
		th1 = threading.Thread(target=self.IsSet)
		th1.start()
		self.parse_area()
		self.parse_plate()
		self.parse_page()
		self.MyThread(self.house_url_queue, self.save_url)
		self.bol = False

if __name__ == '__main__':
	ftx = FangTxProject()
	ftx.run()
