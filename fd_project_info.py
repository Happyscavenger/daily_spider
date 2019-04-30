# coding = utf-8
import requests, time, random
import threading
from queue import Queue
import json, re, pymysql
from copy import deepcopy
from bs4 import BeautifulSoup
import datetime

class ProxyIP:
	''' 代理IP类 '''
	
	def __init__(self):
		self.order = '1ed656e970a1956df7375bab520c2a5f'
		self.apiUrl = 'http://api.ip.data5u.com/dynamic/get.html?order=' + self.order
		self.res = ''
	
	@property
	def GetProxyIp(self):
		''' 获取代理IP '''
		self.res = requests.get(self.apiUrl).text.strip('\n')
		return self.res

class NewFd(object):
	
	def __init__(self):
		self.num = 0
		self.bol = True
		self.pi = ProxyIP()
		self.proxyip = self.pi.GetProxyIp
		self.project_queue = Queue()
		self.presell_queue = Queue()
		self.project_detail_queue = Queue()
		self.event = threading.Event()
		self.start_url = 'http://www.fangdi.com.cn/service/index/getWriteDict.action?dict=dic_lm_512'
		
	@property
	def CurrentTime(self):
		nowtime = time.strftime("%Y-%m-%d %X", time.localtime())
		t = time.strptime(nowtime, '%Y-%m-%d %X')
		y, m, d, h, M = t[0:5]
		newtime = datetime.datetime(y, m, d, h, M)
		return newtime
	
	def Download(self, url):
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
			"User-Agent": ua
		}
		proxies = {'http': 'http://' + self.proxyip}
		try:
			response = requests.get(url, proxies=proxies, headers=headers, timeout=10)
			# print(response.url)
			# res = response.content.decode('utf-8')
			if response.status_code != 200:
				response = ''
		except:
			response = ""
		return response
	
	# 多线程（队列，方法）
	def MyThread(self, myqueue, methodThread):
		tag = 0
		num = 20
		while myqueue.qsize():
			# time.sleep(0.5)
			tag += 1
			print(tag)
			if myqueue.qsize() < 20:
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
	
	def IsSet(self):
		''' 线程运行状态 '''
		while True:
			if not self.event.isSet():
				try:
					self.proxyip = self.pi.GetProxyIp
				except:
					time.sleep(3)
					print('proxyip出错！！！')
					continue
				self.num += 1
				print('IP更换次数', self.num)
				# 标记状态为True
				self.event.set()
			
			time.sleep(3)
			
			if not self.bol:
				break
	
	def parse_area(self):
		while True:
			response = self.Download(self.start_url)
			if response:
				content = response.content.decode('utf-8')
				res = json.loads(content)
				item = {}
				area_list = res['listWriteDict']
				for area in area_list:
					item['area_name'] = area['name']
					item['area_code'] = area['code']
					# if item['area_name'] == '虹口区':
					detail_url = "http://www.fangdi.com.cn/service/freshHouse/getHosueList.action?districtID={}".format(
						item['area_code'])
					item['detail_url'] = detail_url
					print(item)
					self.project_queue.put(deepcopy(item))
				break
			else:
				self.event.clear()
				time.sleep(2)
			self.event.wait()
	
	def parse_project(self):
		item = self.project_queue.get()
		area_url = item['detail_url']
		page = 1
		while True:
			response = self.Download(area_url)
			if response:
				content = response.content.decode('utf-8')
				total_page_num = re.search(r'>共<i>(\d+)<\\/i>页<', content).group(1)
				print(total_page_num)
				while page <= int(total_page_num):
					next_url = area_url + "&currentPage={}".format(page)
					print('----url是%s' % next_url)
					page += 1
					self.presell_queue.put(deepcopy(next_url))
					print(self.presell_queue.qsize())
				break
			else:
				self.event.clear()
				time.sleep(2)
			self.event.wait()

	def get_project(self):
		url = self.presell_queue.get()
		while True:
			response = self.Download(url)
			# if '项目地址' in response and not 'default_row_tr' in response:
			# 	break
			if response:
				content = response.content.decode('utf-8')
				jsons = json.loads(content)
				soup = BeautifulSoup(jsons["htmlView"], features='lxml')
				for info in soup.find_all('tr', {'class': 'default_row_tr'}):
					td = info.find_all('td')
					project_id = td[1].find('a')['onclick'].replace("houseDetail('", "").replace("')", "")
					project_detail = "http://www.fangdi.com.cn/service/freshHouse/queryProjectById.actin?projectID={}".format(
						project_id)
					print(project_detail)
					self.project_detail_queue.put(deepcopy(project_detail))
					print(self.project_detail_queue.qsize())
				break
			else:
				self.event.clear()
				time.sleep(2)
			self.event.wait()
	
	def project_info(self):
		project_detail_url = self.project_detail_queue.get()
		while True:
			response = self.Download(project_detail_url)
			if response:
				content = response.content.decode('utf-8')
				item = {}
				page_info = json.loads(content)
				project = page_info.get('project')
				if project:
					item['项目ID'] = re.search(r'projectID=(.*)', response.url).group(1)
					item['总套数'] = project['num']
					item['总面积'] = project['area']
					item['住宅总套数'] = project['z_num']
					item['住宅面积'] = project['z_area']
					item['可售总套数'] = project['leaving_num']
					item['可售总面积'] = project['leaving_area']
					item['可售住宅套数'] = project['z_leaving_num']
					item['可售住宅面积'] = project['z_leaving_area']
					item['预定总套数'] = project['precon_num']
					item['预定总面积'] = project['precon_area']
					item['预定住宅套数'] = project['z_precon_num']
					item['预定住宅面积'] = project['z_precon_area']
					item['已售总套数'] = project['sell_num']
					item['已售总面积'] = project['sell_area']
					item['已售住宅套数'] = project['z_sell_num']
					item['已售住宅面积'] = project['z_sell_area']
					item['已登记总套数'] = project['reg_num']
					item['已登记总面积'] = project['reg_area']
					item['已登记住宅套数'] = project['z_reg_num']
					item['已登记住宅面积'] = project['z_reg_area']
					item['合同撤销总套数'] = project['cancel_time']
					item['定金逾期总次数'] = project['djcancel_time']
					insert_time = self.CurrentTime
					print(item)
					conn = pymysql.connect(
						host='rm-uf6t4r3u8vea8u3404o.mysql.rds.aliyuncs.com',
						user='caijisa',
						passwd='Caijisa123',
						db='lansi_data_collection',
						port=3306,
						charset='utf8')
					cur = conn.cursor()
					sql = """insert into `fdw_project_info` (`xmid`,`num`,`acreage`,`z_num`,`z_area`,`leaving_num`,`leaving_area`,`z_leaving_num`,`z_leaving_area`,
					`precon_num`,`precon_area`,`z_precon_num`,`z_precon_area`,`sell_num`,`sell_area`,`z_sell_num`,`z_sell_area`,`reg_num`,`reg_area`,`z_reg_num`,`z_reg_area`,
					`cancel_time`,`djcancel_time`,`insert_time`)values('%s',%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,'%s','%s','%s')""" % (
						str(item['项目ID']), item['总套数'], item['总面积'], item['住宅总套数'], item['住宅面积'], item['可售总套数'],
						item['可售总面积'], item['可售住宅套数'],
						item['可售住宅面积'], item['预定总套数'], item['预定总面积'], item['预定住宅套数'], item['预定住宅面积'], item['已售总套数'],
						item['已售总面积'],
						item['已售住宅套数'], item['已售住宅面积'], item['已登记总套数'], item['已登记总面积'], item['已登记住宅套数'],
						item['已登记住宅面积'], str(item['合同撤销总套数']),
						str(item['定金逾期总次数']), insert_time)
					cur.execute(sql)
					conn.commit()
					cur.close()
					conn.close()
					time.sleep(1)
				break
			else:
				self.event.clear()
				time.sleep(2)
			self.event.wait()
	
	def run(self):
		self.event.set()
		th1 = threading.Thread(target=self.IsSet)
		th1.start()
		self.parse_area()
		self.parse_project()
		self.MyThread(self.presell_queue, self.get_project)
		self.MyThread(self.project_detail_queue, self.project_info)
		self.bol = False
		print('爬取完成')


if __name__ == '__main__':
	nfd = NewFd()
	nfd.run()
