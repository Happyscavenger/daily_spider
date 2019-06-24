# -*- coding:utf-8 -*-
import requests
from faker import Faker
from random import choice
import json
import time
import pymysql


class Residence(object):

	# 类属性
	def __init__(self):
		self.url = "http://www.fangdi.com.cn/service/trade/getFirstResidenceStat.action"
		self.conn = pymysql.connect(
			host='rm-uf6t4r3u8vea8u3404o.mysql.rds.aliyuncs.com',
			port=3306,
			user='caijisa',
			passwd='Caijisa123',
			db='lansi_data_collection',
			charset='utf8')
		self.cur = self.conn.cursor()

	# 下载器
	def download(self, url):
		fake_ua = Faker()
		agents = [
			fake_ua.chrome(),
			fake_ua.firefox(),
			fake_ua.opera(),
			fake_ua.safari()]
		agent = choice(agents)
		headers = {'User-Agent': agent}
		response = requests.get(url, headers=headers)
		res = response.content.decode('utf-8')
		return res

	# 解析房地网的响应，并存入mysql
	def parse_data(self, response):
		res_dict = json.loads(response)
		for i in res_dict["FirstResidenceStat"]:
			item = {}
			item['区域'] = i['zonename']
			item['今年已售住宅套数'] = i['z_all_sign_num']
			item['今年已售住宅面积'] = i['z_all_sign_area']
			item['今日已售住宅套数'] = i['z_sign_num']
			item['今日已售住宅面积'] = i['z_sign_area']
			item['今年已售办公套数'] = i['b_all_sign_num']
			item['今年已售办公面积'] = i['b_all_sign_area']
			item['今日已售办公套数'] = i['b_sign_num']
			item['今日已售办公面积'] = i['b_sign_area']
			item['今年已售商业套数'] = i['s_all_sign_num']
			item['今年已售商业面积'] = i['s_all_sign_area']
			item['今日已售商业套数'] = i['s_sign_num']
			item['今日已售商业面积'] = i['s_sign_area']
			item['今年已售其它套数'] = i['q_all_sign_num']
			item['今年已售其它面积'] = i['q_all_sign_area']
			item['今日已售其它套数'] = i['q_sign_num']
			item['今日已售其它面积'] = i['q_sign_area']
			a = time.localtime()
			current_time = time.strftime("%Y-%m-%d", a)
			item['时间'] = current_time
			sql = """replace into `fdw_day_sales_info` (`qy`,`z_all_sign_num`,`z_all_sign_area`,`z_sign_num`,`z_sign_area`,`b_all_sign_num`,`b_all_sign_area`,
			`b_sign_num`,`b_sign_area`,`s_all_sign_num`,`s_all_sign_area`,`s_sign_num`,`s_sign_area`,`q_all_sign_num`,`q_all_sign_area`,`q_sign_num`,`q_sign_area`,
			`date`)VALUES ("%s","%d","%f","%d","%f","%d","%f","%d","%f","%d","%f","%d","%f","%d","%f","%d","%f",str_to_date('%s','%%Y-%%m-%%d'))""" % (
				item['区域'],
				item['今年已售住宅套数'],
				item['今年已售住宅面积'],
				item['今日已售住宅套数'],
				item['今日已售住宅面积'],
				item['今年已售办公套数'],
				item['今年已售办公面积'],
				item['今日已售办公套数'],
				item['今日已售办公面积'],
				item['今年已售商业套数'],
				item['今年已售商业面积'],
				item['今日已售商业套数'],
				item['今日已售商业面积'],
				item['今年已售其它套数'],
				item['今年已售其它面积'],
				item['今日已售其它套数'],
				item['今日已售其它面积'],
				item['时间'])
			self.cur.execute(sql)
			self.conn.commit()

	# 结束运行时自动调用并释放资源
	def __del__(self):
		self.cur.close()
		self.conn.close()

	# run方法启动程序
	def run(self):
		res = self.download(self.url)
		self.parse_data(res)


if __name__ == '__main__':
	residence = Residence()
	residence.run()
