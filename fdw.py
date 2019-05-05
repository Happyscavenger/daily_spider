# coding=utf-8
import re, json, demjson
import threading
from queue import Queue
from copy import deepcopy
from bs4 import BeautifulSoup
import requests, redis, time
from faker import Faker
from random import choice

pool = redis.ConnectionPool(host='r-uf6f097afc7c8814.redis.rds.aliyuncs.com', port=6379, password='13611693775Gx',
                            db=80)
redis = redis.Redis(connection_pool=pool)


def Download(url):
	f = Faker()
	agents = [f.chrome(), f.firefox(), f.opera(), f.safari()]
	agent = choice(agents)
	headers = {'User-Agent': agent}
	proxies = {'http': 'http://' + proxyip}
	# cookies = requests.cookies.RequestsCookieJar()
	# cookies = "FSSBBIl1UgzbN7N7001S=PmzT74pPQg9udZ0ae_Iu5Hj8hPDYOWfeuxDXeVwZC0XipS8_Iq6KgyRAzyKhqSuf; __jsluid=1b61778804653dc4aac4bb3c4c67d201; FSSBBIl1UgzbN7N7001T=3XVr8sVDWoYMYMjyO1S6N7h3ssQwGipd6G4JaVee7Iu8WPPv8vzfMPjK0k7CbMt.dH1LB4lrQvxYL_yEc3BVCBpup1zXD3FtY2jN6r_l5hfg.eLELaUDyxQFXW0ndA_N0pR1dt_P4vg4vmWo9W4T19uVntWrRFMwTEQC_HLeAKopw8dahAXwlpQRP3YxGgqVzGM7fL.qCDG5wzz9ih7Yvk9jt2MavL.KKTtLOdDFv0XXUI.sHgUjiTuxDaqHvF8iGQZ5ExpMLWT8As3Ep3uD9xGomTWxZUGH5oskWk8kdref5q5jSylYb2RMnDXL_KvdBVkPiL_kxZhJ2IN42ViPTQirk; Hm_lvt_d7682ab43891c68a00de46e9ce5b76aa=1551419542,1551922131; JSESSIONID1=9KWFnNfUTPvFMWgqElbDMjpqen_03Vwpdz-XzLgtcVeNmLHZn1X0!1187294111"
	try:
		response = requests.get(url, proxies=proxies, headers=headers, timeout=50)
		# print(response.url)
		res = response.content.decode('utf-8')
		if response.status_code != 200:
			res = ''
		else:
			if "读取出现异常" in response.text:
				res = ""
	except:
		res = ''
	return res


def get_area():
	start_url = 'http://www.fangdi.com.cn/service/index/getWriteDict.action?dict=dic_lm_512'
	while True:
		page_source = Download(start_url)
		item = {}
		if page_source:
			res = json.loads(page_source)
			area_list = res['listWriteDict']
			for area in area_list:
				item['area_name'] = area['name']
				item['area_code'] = area['code']
				# if item['area_name'] in ['青浦区', '金山区']:
					# if item['area_name'] == '青浦区':
				detail_url = "http://www.fangdi.com.cn/service/freshHouse/getHosueList.action?districtID={}".format(
					item['area_code'])
				item['detail_url'] = detail_url
				print(item)
				project_queue.put(deepcopy(item))
			break
		else:
			event.clear()
			time.sleep(1)
		event.wait()


def parse_page():
	while project_queue.qsize():
		item = project_queue.get()
		area_url = item['detail_url']
		page = 0
		while True:
			content = Download(area_url)
			if content:
				break
			else:
				event.clear()
				time.sleep(1)
			event.wait()
		if content:
			total_page_num = re.search(r'>共<i>(\d+)<\\/i>页<', content).group(1)
			print(total_page_num)
			while page < int(total_page_num):
				next_page_num = page + 1
				next_url = area_url + "&currentPage={}".format(next_page_num)
				print('----url是%s' % next_url)
				page += 1
				projects_queue.put(next_url)


def get_project():
	while projects_queue.qsize():
		url = projects_queue.get()
		while True:
			try:
				response = Download(url)
				jsons = json.loads(response)
				soup = BeautifulSoup(jsons["htmlView"], features='lxml')
				break
			except:
				event.clear()
				time.sleep(1)
			event.wait()
		item = {}
		for info in soup.find_all('tr', {'class': 'default_row_tr'}):
			td = info.find_all('td')
			item['ID'] = td[1].find('a')['onclick'].replace("houseDetail('", "").replace("')", "")
			item['状态'] = td[0].text
			if item['状态'] == '1':
				item['状态'] = '即将开盘'
			elif item['状态'] == '2':
				item['状态'] = '在售'
			elif item['状态'] == '3':
				item['状态'] = '即将开售,在售'
			elif item['状态'] == '4':
				item['状态'] = '售完'
			elif item['状态'] == '5':
				item['状态'] = '即将开售,售完'
			elif item['状态'] == '6':
				item['状态'] = '在售,售完'
			elif item['状态'] == '7':
				item['状态'] = '即将开售,在售,售完'
			elif item['状态'] == '8':
				item['状态'] = '暂停销售'
			elif item['状态'] == '9':
				item['状态'] = '即将开售,暂停销售'
			elif item['状态'] == '10':
				item['状态'] = '在售,暂停销售'
			elif item['状态'] == '11':
				item['状态'] = '即将开售,在售,暂停销售'
			elif item['状态'] == '12':
				item['状态'] = '售完,暂停销售'
			elif item['状态'] == '13':
				item['状态'] = '即将开售,售完,暂停销售'
			elif item['状态'] == '14':
				item['状态'] = '在售,售完,暂停销售'
			elif item['状态'] == '15':
				item['状态'] = '即将开售,在售,售完,暂停销售'
			else:
				item['状态'] = ''
			item['项目名称'] = td[1].text
			item['项目地址'] = td[2].text
			item['项目总套数'] = td[3].text
			item['项目总面积'] = td[4].text
			item['区域'] = td[5].text.replace('新区', '').replace('区', '')
			parse_area_url = "http://www.fangdi.com.cn/service/freshHouse/queryStartUnit.actin?projectID={}".format(
				deepcopy(item['ID']))
			item['parse_area_url'] = parse_area_url
			print(item)
			presell_queue.put(deepcopy(item))
			print(presell_queue.qsize())


def parse_presell():
	while presell_queue.qsize():
		item = presell_queue.get()
		item['ID'] = item['ID']
		url = item['parse_area_url']
		while True:
			con = Download(url)
			if con:
				break
			else:
				event.clear()
				time.sleep(1)
			event.wait()
		if re.match(r'.*?(\{.*\}).*', con, re.S):
			dict_l = re.match(r'.*?(\{.*\}).*', con, re.S).group(1)
			dict_l = json.loads(dict_l)
			house = dict_l['priceInformationList']
			yszs = dict_l['startUnitList']
			
			def presell():
				for ysz in yszs:
					item['编号'] = ysz['start_code']
					item['presell_id'] = ysz['presell_id']
					item['许可证'] = ysz['presell_desc']
					# item['开盘日期'] = ysz['start_date']
					item['总套数'] = ysz['num']
					item['住宅总套数'] = ysz['z_num']
					item['总面积'] = ysz['area']
					item['住宅面积'] = ysz['z_area']
					item['预售证状态'] = ysz['status']
					item['预售证ID'] = ysz['start_id']
					yield deepcopy(item)
			
			infos = presell()
			for info in infos:
				for blds in house:
					if blds:
						for bld in blds:
							item['start_id'] = bld['start_id']
							if info['预售证ID'] == item['start_id']:
								item['building_id'] = bld['building_id']
								item['楼栋名称'] = bld['building_name']
								item['bld_总套数'] = bld['num']
								item['bld_总面积'] = bld['area']
								item['开盘日期'] = bld['start_date']['time'] / 1000
								item['开盘日期'] = time.localtime(item['开盘日期'])
								item['开盘日期'] = time.strftime("%Y-%m-%d", item['开盘日期'])
								try:
									item['最高报价'] = bld['hign_refprice']
									item['最低报价'] = bld['low_refprice']
								except KeyError:
									item['最高报价'] = bld['reference_price']
									item['最低报价'] = bld['reference_price']
								print(item)
								parse_houses_detail_url = "http://www.fangdi.com.cn/service/freshHouse/getMoreInfo.action?project_id=%s&buildingID=%s&startID=%s" % \
								                          (item['ID'], item['building_id'], item['start_id'])
								item['parse_houses_detail_url'] = parse_houses_detail_url
								item_queue.put(deepcopy(item))


def parse_build():
	while item_queue.qsize():
		item = item_queue.get()
		url = item['parse_houses_detail_url']
		while True:
			cont = Download(url)
			if cont:
				break
			else:
				event.clear()
				time.sleep(1)
			event.wait()
		try:
			dict_house = json.loads(cont)
		except:
			dict_house = demjson.decode(cont)
		dict_house_list = dict_house['moreInfoList']
		if dict_house_list is not None:
			for key in dict_house_list.keys():
				item['楼层'] = key
				for c in dict_house_list[key]:
					if len(c) > 3:
						item['房子号码'] = c['room_number']
						item['预测面积'] = c['plan_flarea']
						item['实测面积'] = c['flarea']
						item['house_id'] = c['house_id']
						item['销售状态'] = str(c['status'])
						item['签约日期'] = c['last_modi_time']
						d = item['签约日期']['time']
						if type(d) is int:
							time_count = d / 1000
							time_local = time.localtime(time_count)
							item['签约时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
						if item['销售状态'] == '2':
							item['销售状态'] = '已签'
						elif item['销售状态'] == '3':
							item['销售状态'] = '已登记'
						elif item['销售状态'] == '4':
							item['销售状态'] = '可售'
						elif item['销售状态'] == '8':
							item['销售状态'] = '已付定金'
						elif item['销售状态'] == '9':
							item['销售状态'] = '未纳入网上销售'
						elif item['销售状态'] == '-1':
							item['销售状态'] = '属于其它开盘单元'
						item['房屋性质'] = c['house_type']
						if item['房屋性质'] == 1:
							item['房屋性质'] = '动迁房'
						elif item['房屋性质'] == 2:
							item['房屋性质'] = '配套商品房'
						elif item['房屋性质'] == 11:
							item['房屋性质'] = '动迁安置房'
						else:
							item['房屋性质'] = ''
						a = time.localtime()
						current_time = time.strftime("%Y-%m-%d %H:%M", a)
						item['current_time'] = current_time
						item = deepcopy(item)
						print('---' * 50)
						print(item)
						items = json.dumps(item)
						redis.sadd('item', items)


# 多线程（队列，方法）
def MyThread(myqueue, methodThread):
	tag = 0
	num = 40
	while myqueue.qsize() > 0:
		# time.sleep(0.5)
		tag += 1
		print(tag)
		if methodThread == "":
			break
		if myqueue.qsize() < 40:
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
		
		time.sleep(2)


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
			# 标记状态为True
			event.set()
		
		time.sleep(3)
		
		if not bol:
			break


class ProxyIP:
	''' 代理IP类 '''
	
	def __init__(self):
		self.order = 'd86502654f7017ffbe125ae0139afef5'
		self.apiUrl = 'http://api.ip.data5u.com/dynamic/get.html?order=' + self.order
		self.res = ''
	
	@property
	def GetProxyIp(self):
		''' 获取代理IP '''
		self.res = requests.get(self.apiUrl).text.strip('\n')
		return self.res


if __name__ == '__main__':
	while True:
		# for i in range(0,5):
		#	print("第{}轮".format(i+1))
		info = {}
		num = 0
		project_queue = Queue()
		projects_queue = Queue()
		presell_queue = Queue()
		item_queue = Queue()
		event = threading.Event()
		event.set()
		bol = True
		pi = ProxyIP()
		proxyip = pi.GetProxyIp  # 代理IP
		th1 = threading.Thread(target=IsSet)
		th1.start()
		get_area()
		MyThread(project_queue, parse_page)
		MyThread(projects_queue, get_project)
		MyThread(presell_queue, parse_presell)
		MyThread(item_queue, parse_build)
		print('爬取完成')
		bol = False
		time.sleep(15 * 60)
