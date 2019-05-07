# coding = utf-8
import requests
from lxml import etree
from queue import Queue
import re, time, threading
import xlsxwriter


class BanBan(object):
	def __init__(self):
		self.url = "https://www.51banban.com/loupan/ap2"
		self.headers = {
			"user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"}
		self.url_queue = Queue()
		self.detail_url_queue = Queue()
		self.rec_data = []
	
	def download(self, url):
		response = requests.get(url, headers=self.headers)
		res = response.content.decode("utf-8")
		return res
	
	def next_url(self):
		res = self.download(self.url)
		html = etree.HTML(res)
		total_page_url = html.xpath('//a[text()="末页"]/@href')[0]
		total_page = int(re.findall("https://www.51banban.com/loupan/ap2/p(\d+)/", total_page_url)[0])
		page = 0
		while page < total_page:
			next_page_num = page + 1
			next_url = "https://www.51banban.com/loupan/ap2/p{}".format(next_page_num)
			self.url_queue.put(next_url)
			print(next_url, self.url_queue.qsize())
			page += 1
	
	def parse_url(self):
		while self.url_queue.qsize():
			url = self.url_queue.get()
			res = self.download(url)
			html = etree.HTML(res)
			h4_list = html.xpath('//div[@class="office_con"]/div/div[@class="property_text"]/h4')
			for a in h4_list:
				detail_url = a.xpath("./a/@href")[0]
				self.detail_url_queue.put(detail_url)
				print(detail_url)
			time.sleep(1)
	
	def parse_detail(self):
		while self.detail_url_queue.qsize():
			url = self.detail_url_queue.get()
			res = self.download(url)
			html = etree.HTML(res)
			item = {}
			item['区域'] = html.xpath("//div[@class='overview_infor']/div[2]/p/a[1]/text()")[0]
			item['楼盘名'] = html.xpath('//p[text()="楼盘名："]/strong/text()')[0]
			item['地址'] = html.xpath("//h2/span/text()")[0]
			if " · " in item['地址']:
				item['地址'] = re.search(r"· (.*)", item["地址"]).group(1).strip()
			item['竣工日期'] = html.xpath('//div[@class="overview_con"]/p/text()')[0]
			item['竣工日期'] = re.sub("竣\xa0\xa0\xa0\xa0\xa0工 ：", "", item['竣工日期'])
			item['楼盘描述'] = html.xpath('//div[@id="tab_con1"]/div/text()')[1].strip()
			item['楼盘描述'] = re.sub("\r\n", "", item['楼盘描述'])
			print(item)
			self.rec_data.append(item)
	
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
		worksheet.write('C1', '地址', bold_format)
		worksheet.write('D1', '竣工日期', bold_format)
		worksheet.write('E1', '楼盘描述', bold_format)
		# worksheet.write('F1', 'id_2_doc', bold_format)
		row = 1
		# col = 0
		for item in (self.rec_data):
			worksheet.write_string(row, 0, str(item['区域']))
			worksheet.write_string(row, 1, item['楼盘名'])
			worksheet.write_string(row, 2, str(item['地址']))
			worksheet.write_string(row, 3, item['竣工日期'])
			worksheet.write_string(row, 4, str(item['楼盘描述']))
			row += 1
		workbook.close()
	
	def my_threading(self, myqueue, mythodThread):
		tag = 0
		num = 5
		while myqueue.qsize():
			tag += 1
			print(tag)
			if myqueue.qsize() < 5:
				num = myqueue.qsize()
			threads = []
			for i in range(num):
				th = threading.Thread(target=mythodThread)
				# 加入线程列表
				threads.append(th)
			
			# 开始线程
			for i in range(num):
				threads[i].setDaemon(True)
				threads[i].start()
			
			# 结束线程
			for i in range(num):
				threads[i].join()
			time.sleep(0.5)
	
	def run(self):
		self.next_url()
		self.parse_url()
		# self.parse_detail()
		self.my_threading(self.detail_url_queue, self.parse_detail)
		self.generate_excel()

if __name__ == '__main__':
	banban = BanBan()
	banban.run()
