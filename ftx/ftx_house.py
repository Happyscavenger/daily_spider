import requests, re, queue, threading, pymysql, time, sys
from bs4 import BeautifulSoup
from pymongo import MongoClient
from lxml import etree
import random,sys


class FangTxHouse(object):
	
	def __init__(self):
		self.order = 'ef8e072865d9e1afdd085731eb44f5d6'
		self.apiUrl = 'http://api.ip.data5u.com/dynamic/get.html?order=' + self.order
		self.res = ''
		self.num = 0  # ip 更换计数
		self.event = threading.Event()
		self.bol = True  # 控制线程退出
		self.proxyip = self.GetProxyIp  # 代理IP
		self.url_queue = queue.Queue()
		client = MongoClient(
			'mongodb://root:Lansi123@dds-uf605bb40eca92541596-pub.mongodb.rds.aliyuncs.com:3717,dds-uf605bb40eca92542332-pub.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-4720883')
		db = client.caiji
		self.ftx = db.fangtianxia
	
	@property
	def GetProxyIp(self):
		''' 获取代理IP '''
		self.res = requests.get(self.apiUrl).text.strip('\n')
		return self.res
	
	def Download(self,url):
		''' 页面下载 '''
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
		# "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36"
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
		proxies = {'http': 'http://' + self.proxyip, "https": "https://" + self.proxyip}
		try:
			response = requests.get(url, headers=headers, proxies=proxies, timeout=5)
			# response.encoding = 'GBK'
			# soup = BeautifulSoup(response.text, 'lxml')
			res = response.text
		except:
			info = sys.exc_info()
			if not 'timed out' in str(info[1]):
				print(threading.currentThread().getName(), info[1])
			return ''
		return res
	
	def IsSet(self):
		''' 线程运行状态 '''
		while True:
			if not self.event.isSet():  # 线程阻塞，更换IP
				try:
					self.proxyip = self.GetProxyIp
				except:
					time.sleep(3)
					print('proxyip出错！！！')
					continue
				self.num += 1
				print('IP更换次数', self.num)
				self.event.set()  # 标记状态为True
			time.sleep(3)
			if not self.bol:
				break
	
	# 从mongo中获取url
	def push_url(self):
		for i in self.ftx.find({"tag": ''}).sort('url', 1):
			self.url_queue.put(i['url'])
	
	# 解析具体室号内信息
	def parse_house(self):
		htmlUrl = self.url_queue.get()
		while True:
			res = self.Download('http://esf.sh.fang.com' + htmlUrl)
			if res:
				if '未找到相关页面' in res:
					print('http://esf.sh.fang.com' + htmlUrl)
					self.ftx.delete_one({'url': htmlUrl})
				elif not 'pageConfig.district =' in res:
					return
				break
			else:
				self.event.clear()
			self.event.wait()
		info = {}
		if 'pageConfig.district =' in res:
			info['来源'] = '房天下'
			try:
				info['区域'] = eval(re.findall('pageConfig.district =(.*?);', res)[0])
			except:
				info['区域'] = ""
				with open("err.txt", "a") as f:
					f.write("区域错误: " + "http://esf.sh.fang.com" + htmlUrl + "\n")
			info['地址'] = re.findall("pageConfig.address='(.*?)'", res)[0]
			try:
				info['小区'] = eval(re.findall('pageConfig.projname = (.*?);', res)[0])
			except:
				info['小区'] = ""
				with open("err.txt", "a") as f:
					f.write("小区错误: " + "http://esf.sh.fang.com" + htmlUrl + "\n")
			info['年代'] = eval(re.findall("pageConfig.createtime=(.*?);", res)[0])
			info['朝向'] = eval(re.findall("pageConfig.forward=(.*?);", res)[0])
			info['面积'] = eval(re.findall('pageConfig.area=(.*?);', res)[0])
			info['楼层'] = eval(re.findall('pageConfig.floor=(.*?);', res)[0])
			info['总高'] = eval(re.findall('pageConfig.totalfloor=(.*?);', res)[0])
			try:
				info['总价'] = int(float(eval(re.findall('pageConfig.price=(.*?);', res)[0])) * 10000)
			except:
				info['总价'] = 0
			info['单价'] = eval(re.findall('unitprice:(.*?),', res)[0])
			info['姓名'] = eval(re.findall('pageConfig.agentname=(.*?);', res)[0])
			# info['电话'] = soup.find('span', {'id': 'phone'}).text
			html = etree.HTML(res)
			info['电话'] = html.xpath("//span[@id='mobilecode']/text()")
			info['电话'] = info['电话'][0] if info['电话'] else None
			info['户型'] = html.xpath("//div[text()='户型']/preceding-sibling::div[1]/text()")
			info['户型'] = info['户型'][0].strip() if info['户型'] else ""
			# info['装修'] = html.xpath("//div[text()='装修']/preceding-sibling::*[1]/text()")[0]
			infos = html.xpath("//div[@class='tab-cont-right']/div/div[@style='border-right: 0;']/div/text()")
			info['装修'] = infos[2]
			a = html.xpath("//div[@class='cont clearfix']/div/span/text()")
			info['类别'] = ''
			for i in range(len(a)):
				if a[i].strip() == "建筑类别":
					info['类别'] = a[i + 1]
					break
			# info['类别'] = html.xpath("//span[text()='建筑类别']/following-sibling::span[1]/text()")
			info['中介'] = re.findall("pageConfig.comname='(.*)?'", res)[0].strip()
			info['类型'] = re.findall("pageConfig.purpose='(.*)?'", res)[0].strip()
			info['URL'] = 'http://esf.sh.fang.com' + htmlUrl
			c_time = str(time.strftime("%Y-%m-%d"))
			print(info)
			try:
				conn = pymysql.connect(
					host='rm-uf6t4r3u8vea8u3404o.mysql.rds.aliyuncs.com',
					port=3306,
					user='caijisa',
					passwd='Caijisa123',
					db='lansi_data_collection',
					charset='utf8')
				cursor = conn.cursor()
				cursor.callproc('CJ_fangtianxia',
				                (info['区域'], info['地址'], info['小区'], info['总价'], info['单价'], info['面积'], info['总高'],
				                 info['楼层'], info['朝向'], info['装修'], info['年代'], info['来源'], info['URL'], c_time,
				                 info['类型'], info['户型'], info['姓名'], info['中介'], info['电话'], info['类别']))
				conn.commit()
				cursor.close()
				conn.close()
				self.ftx.update_one({'url': htmlUrl}, {'$set': {
					'tag': '1'
				}})
			except:
				pass
	
	# 多线程（队列，方法）
	def MyThread(self, myqueue, methodThread):
		tag = 0
		while myqueue.qsize():
			num = 50
			if myqueue.qsize() < num:
				num = myqueue.qsize()
			threads = []  # 线程列表
			for i in range(num):
				th = threading.Thread(target=methodThread)
				threads.append(th)  # 加入线程列表
			
			for i in range(num):  # 开始线程
				threads[i].start()
			
			for i in range(num):  # 结束线程
				threads[i].join()
			tag += 1
	
	# 执行程序
	def run(self):
		self.event.set()
		# 更换IP线程
		th1 = threading.Thread(target=self.IsSet)
		th1.start()
		self.push_url()
		self.MyThread(self.url_queue, self.parse_house)
		self.bol = False  # 退出线程


if __name__ == '__main__':
	ftx_house = FangTxHouse()
	ftx_house.run()
