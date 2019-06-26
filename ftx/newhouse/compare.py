# -*- coding:utf-8 -*-
from pymongo import MongoClient
import pymysql
import datetime
import time
import re
from faker import Faker
from random import choice
import requests
import json


class CompareNewHouse(object):

	def __init__(self):
		self.url = "http://www.fangdi.com.cn//service/trade/getHouseReview.action"
		self.conn = pymysql.connect(
			host='rm-uf6t4r3u8vea8u3404o.mysql.rds.aliyuncs.com',
			port=3306,
			user='caijisa',
			passwd='Caijisa123',
			db='lansi_data_collection',
			charset='utf8',
			cursorclass=pymysql.cursors.DictCursor
		)
		self.cursor = self.conn.cursor()
		self.client = MongoClient(
			"mongodb://root:Lansi123@dds-uf605bb40eca92541596-pub.mongodb.rds.aliyuncs.com:3717,"
			"dds-uf605bb40eca92542332-pub.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-4720883")
		self.db = self.client.caiji_new
		self.newroom = self.db.NewRoom

	# 返回固定时间格式
	@property
	def parse_time(self):
		# time_str = datetime.datetime.now()
		# dateArray = time_str.strftime("%Y-%m-%d 00:00:00")
		timeStamp = int(time.time())
		dateArray = datetime.datetime.utcfromtimestamp(timeStamp)
		datedel = datetime.timedelta(days=1)
		dateStr = dateArray - datedel
		yesterday = dateStr.strftime("%Y-%m-%d 00:00:00")
		today = dateArray.strftime("%Y-%m-%d 00:00:00")
		# print(yesterday)
		# print(today)
		timeDict = {}
		timeDict['yesterday'] = yesterday
		timeDict['today'] = today
		return timeDict

	# 请求方法
	def download(self, url):
		f = Faker()
		agents = [f.chrome(), f.firefox(), f.opera(), f.safari()]
		agent = choice(agents)
		headers = {
			"User-Agent": agent,
			"Accept - Encoding": "gzip, deflate",
			"Accept - Language": "zh - CN, zh;q = 0.9",
			"Cache - Control": "no - cache",
			"Host": "www.fangdi.com.cn"
		}
		response = requests.get(url, headers=headers)
		res = response.content.decode("utf-8")
		return res

	# 返回昨日采集入mongo里的数据
	def parse(self, area):
		timeDict = self.parse_time
		yesterday = timeDict['yesterday']
		today = timeDict['today']
		if area:
			num = self.newroom.count_documents(
				{'QY': area, 'YGQYSJ': {
					"$gt": '{}'.format(yesterday),
					"$lt": "{}".format(today),
					"$not": re.compile(
						".*02:00:.*|.*02:01:.*|.*01:59:.*",
						re.S)}})
		else:
			num = self.newroom.count_documents(
				{
					'YGQYSJ': {
						"$gt": '{}'.format(yesterday),
						"$lt": "{}".format(today),
						"$not": re.compile(
							".*02:00:.*|.*02:01:.*|.*01:59:.*",
							re.S)}})
			# print(num)
		return num

	# 访问昨日交易统计
	def parse_count(self):
		item = {}
		res = self.download(self.url)
		info = json.loads(res)
		item['listHouseReview6'] = info['listHouseReview6'][0]
		item['listHouseReview3'] = info['listHouseReview3'][0]
		item['listHouseReview2'] = info['listHouseReview2'][0]
		item['listHouseReview5'] = info['listHouseReview5']
		item['listHouseReview4'] = info['listHouseReview4'][0]
		item['listHouseReview'] = info['listHouseReview'][0]
		item['sysDate'] = info['sysDate']
		item['todayselledarea'] = info['todayselledarea']
		item['todayselledcount'] = info['todayselledcount']
		# print(item)
		count = item['listHouseReview3']['count'] + item['listHouseReview2']['count'] + \
				item['listHouseReview4']['count'] + item['listHouseReview']['count']
		print(count)
		area = ""
		num = self.parse(area)
		if 5 >= num - count >= -5:
			return False
		else:
			return True

	# 返回昨日各区域房地网新房销售统计数量
	def parse_area(self):
		sql = """select `qy`,`z_sign_num`as "z_num",`b_sign_num` as 'b_num',`s_sign_num`as 's_num',
		`q_sign_num` as 'q_num'from `fdw_day_sales_info`"""
		self.cursor.execute(sql)
		results = self.cursor.fetchall()
		for result in results:
			item = {}
			if result['qy'] != "全市":
				item['count'] = result['z_num'] + result['b_num'] + \
								result['s_num'] + result['q_num']
				item['qy'] = result['qy']
				yield item
	# 区域销售对比
	def compare_area(self):
		items = self.parse_area()
		for item in items:
			print(item)
			area = item['qy']
			if "新区" in area:
				area = re.search("(\w+)新区", area).group(1)
			else:
				area = re.search("(\w+)区", area).group(1)
			area_count = item['count']
			count_mongo = self.parse(area)
			print(count_mongo)
			num = area_count - count_mongo
			if num <= -3:
				print("{}数据异常,采集多{}套".format(area, -num))
			elif num >= 3:
				print("{}数据异常,采集少{}套".format(area, num))
			else:
				print("{}正常".format(area))


	#资源回收
	def __del__(self):
		self.cursor.close()
		self.conn.close()
		self.client.close()

	def run(self):
		if self.parse_count():
			self.compare_area()
		else:
			print("数据总量正常")


if __name__ == '__main__':
	cnh = CompareNewHouse()
	cnh.run()
