# -*- coding: utf-8 -*-
import requests
import random,sys,threading
from lxml import etree
from bs4 import BeautifulSoup
from queue import Queue
import re,time,xlsxwriter
from copy import deepcopy
import pymysql


class Bussiness(object):
	
	def __init__(self):
		self.start_url = "https://sh.lianjia.com/ershoufang/sf2"
		self.url_queue =  Queue()
		self.detail_queue =Queue()
		self.detail_url_queue = Queue()
		self.rec_data = []
	def get_ua(self):
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
		return ua
	
	def download(self, url):
		''' 页面下载 '''
		ua = self.get_ua()
		headers = {
			'User-Agent': ua}
		
		# proxies = {'http': 'http://' + self.proxyip}
		# cookies = requests.cookies.RequestsCookieJar()  # cookies=cookies
		try:
			response = requests.get(url,headers=headers, timeout=10)  # cookies=cookies,
			# cookies.update(response.cookies)
			res = response.content.decode('utf-8')
			html = etree.HTML(res)
			soup = BeautifulSoup(response.text, features='lxml')
			item = {'soup': soup, 'html': html}
		except:
			info = sys.exc_info()
			if not 'timed out' in str(info[1]):
				print(threading.currentThread().getName(), info[1])
			item = ""
		return item
	
	def MyThread(self, myqueue, methodThread):
		''' 多线程（队列，方法）'''
		num = 0
		while myqueue.qsize():
			threads = []
			num += 1
			print(num)
			ts = 60  # 线程数
			if myqueue.qsize() < ts:
				ts = myqueue.qsize()
			for i in range(ts):
				th = threading.Thread(target=methodThread)
				# 加入线程列表
				threads.append(th)
			print('----------------------')
			# 开始线程
			for i in range(ts):
				threads[i].start()
			
			# 结束线程
			for i in range(ts):
				threads[i].join()
			print('----------------------')
			time.sleep(1)
			
	def parse_url(self):
		page = 1
		total_page = 93
		left_url = self.start_url[:-3]
		right_url = self.start_url[-3:]
		while page <= total_page:
			next_url = left_url + "pg{}".format(page) + right_url
			self.url_queue.put(next_url)
			page += 1
			print(self.url_queue.qsize())
			
	def push_url(self):
		while self.url_queue.qsize():
			url = self.url_queue.get()
			info = self.download(url)
			if info:
				html = info['html']
				li_list = html.xpath('//ul[@class="sellListContent"]/li')
				for li in li_list:
					detail_url = li.xpath("./a/@href")[0]
					self.detail_url_queue.put(deepcopy(detail_url))
					print(self.detail_url_queue.qsize())
					
	def parse_detail(self):
		url = self.detail_url_queue.get()
		info = self.download(url)
		if info:
			html = info['html']
			soup = info['soup']
			item = {}
			item['area'] = html.xpath('//div[@class="areaName"]/span[@class="info"]/a[1]/text()')[0]
			item['propertyname'] = re.findall("resblockName:'(.*)?',", str(soup))
			item['propertyname'] = item['propertyname'][0] if item['propertyname'] else ""
			item["ladder_ratio"] = re.findall(r"梯户比例</span>(.*)?</li>",str(soup))[0]
			item['Property_right_years'] = re.findall(r"产权年限</span>(.*)?</li>",str(soup))[0]
			#房屋用途
			item['house_use'] = html.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[4]/span[2]/text()')[0]
			#用水类型
			item['water_use'] = re.findall(r"用水类型</span>(.*)?</li>",str(soup))[0]
			# 用电类型
			item['power_consumption'] = re.findall(r"用电类型</span>(.*)?</li>",str(soup))[0]
			item['house_id'] = re.findall(r"houseId:'(\d+)',",str(soup))[0]
			item['rid_id'] = re.findall(r"resblockId:'(\d+)',",str(soup))[0]
			self.detail_queue.put(deepcopy(item))
			print(item)
			
	def parse_build(self):
		item = self.detail_queue.get()
		house_id = item['house_id']
		rid_id = item['rid_id']
		url = "https://sh.lianjia.com/ershoufang/housestat?hid={}&rid={}".format(house_id,rid_id)
		info = self.download(url)
		if info:
			soup = info['soup']
			item['build_year'] = re.findall(r'"buildYear":"(.*?)",',str(soup))[0].encode('utf-8').decode("unicode_escape")
			item['buildNum'] = re.findall(r'"buildNum":"(.*?)",', str(soup))[0].encode('utf-8').decode("unicode_escape")
			item['unitPrice'] = re.findall(r'"unitPrice":(.*?),', str(soup))[0].encode('utf-8').decode("unicode_escape")
			conn = pymysql.connect(
				host='rm-uf6t4r3u8vea8u3404o.mysql.rds.aliyuncs.com',
				port=3306,
				user='caijisa',
				passwd='Caijisa123',
				db='lansi_data_collection',
				charset='utf8'
			)
			cur = conn.cursor()
			sql = "select `Address` from `ljproperty` where PropertyNO ='{}'".format(str(item['rid_id']))
			cur.execute(sql)
			data = cur.fetchall()
			try:
				item['address'] = data[0][0]
			except:
				with open("err.txt","a") as f:
					f.write(url+":" +"{}".format(item) + "\n" )
			conn.commit()
			cur.close()
			conn.close()
			time.sleep(1)
			self.rec_data.append(item)
			print(item)
	
	def generate_excel(self):
		workbook = xlsxwriter.Workbook('./banban.xlsx')
		worksheet = workbook.add_worksheet()
		
		# 设定格式，等号左边格式名称自定义，字典中格式为指定选项
		# bold：加粗，num_format:数字格式
		bold_format = workbook.add_format({'bold': True})
		# money_format = workbook.add_format({'num_format': '$#,##0'})
		# date_format = workbook.add_format({'num_format': 'mmmm d yyyy'})
		
		# 将二行二列设置宽度为15(从0开始)
		worksheet.set_column(1, 1, 15)
		
		# 用符号标记位置，例如：A列1行
		worksheet.write('A1', '区域', bold_format)
		worksheet.write('B1', '楼盘名', bold_format)
		worksheet.write('C1', '梯户比例', bold_format)
		worksheet.write('D1', '产权年限', bold_format)
		worksheet.write('E1', '房屋用途', bold_format)
		worksheet.write('F1', '用水类型', bold_format)
		worksheet.write('G1', '用电类型', bold_format)
		worksheet.write('H1', '建筑年代', bold_format)
		worksheet.write('I1', '楼栋总数', bold_format)
		worksheet.write('J1', '小区均价', bold_format)
		worksheet.write('K1', '地址', bold_format)
		row = 1
		# col = 0
		for item in (self.rec_data):
			worksheet.write_string(row, 0, str(item['area']))
			worksheet.write_string(row, 1, item['propertyname'])
			worksheet.write_string(row, 2, str(item['ladder_ratio']))
			worksheet.write_string(row, 3, item['Property_right_years'])
			worksheet.write_string(row, 4, str(item['house_use']))
			worksheet.write_string(row, 5, str(item['water_use']))
			worksheet.write_string(row, 6, str(item['power_consumption']))
			worksheet.write_string(row, 7, str(item['build_year']))
			worksheet.write_string(row, 8, str(item['buildNum']))
			worksheet.write_string(row, 9, str(item['unitPrice']))
			worksheet.write_string(row, 10, str(item['address']))
			row += 1
		workbook.close()
	
	
	def run(self):
		self.parse_url()
		self.push_url()
		self.MyThread(self.detail_url_queue,self.parse_detail)
		self.MyThread(self.detail_queue,self.parse_build)
		self.generate_excel()

		
if __name__ == '__main__':
	bn = Bussiness()
	bn.run()