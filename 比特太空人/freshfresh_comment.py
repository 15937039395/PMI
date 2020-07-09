'''
    两鲜评论
    数据量不大,产品id加载到内存---> 可修改
'''
import requests
import time
import datetime
import re
from pymongo import MongoClient

try:
    from urllib.parse import quote_plus, urlencode,urljoin
except:
    from urllib import quote_plus, urlencode

IP = '192.168.0.11'
PORT = '27017'
USERNAME = 'liushizhan'
PASSWORD = 'liushizhan'

uri = "mongodb://%s:%s@%s:%s" % (quote_plus(USERNAME), quote_plus(PASSWORD), IP, PORT)

class FreshFreshComment:
    def __init__(self, start='2018-1-1 0:0:0', end='2018-1-31 23:59:59', dbname='Fresh', getExcept=False):
        self.url = 'http://120.76.205.241:8000/comment/freshfresh'
        self.params = {
            'id':'',
            'apikey':'iyRRRaIDWDou6yiDZqAOL9WU03ijEXJfYVxcfvihYOi8BzYE31FoOLovWgHCbIeg',
        }
        self.client = MongoClient(uri)
        self.db = self.client[dbname]
        # 程序是否异常终止
        self.getExcept = getExcept
        # 生成集合名
        tm = time.localtime()
        # 根据当前抓取日期生成集合名称
        self.coll_name = 'freshFreshComment_%s-%s'%(tm.tm_year,tm.tm_mon)
        # 生成评论截止日期
        # self.startTime = time.strptime(start, '%Y-%m-%d %H:%M:%S')
        # self.endTime = time.strptime(end, '%Y-%m-%d %H:%M:%S')

        self.startTime, self.endTime = self.get_time_interval()
        # 上次正爬取的url转入错误集合
        cursor = self.db[self.coll_name+'_run'].find()
        for cur in cursor:
            cur['errCode'] = '异常终止'
            self.db[self.coll_name+'_err'].insert(cur)
        cursor.close()
        self.db[self.coll_name + '_run'].remove()

    def get_time_interval(self):
        # 获取时间区间
        year = datetime.datetime.today().year
        month = datetime.datetime.today().month
        ed = (datetime.datetime(year, month, 1) - datetime.timedelta(seconds=1)).timetuple()
        if month == 1:
            year = year - 1
            month = 13
        st = datetime.datetime(year, month - 1, 1).timetuple()
        return st,ed

    def get(self, url, pid, params=None):
        self.db[self.coll_name+'_run'].remove()
        if params:
            self.insert_run_url(
                url=url + '?' + urlencode(self.params),
                pid=pid
            )
        else:
            self.insert_run_url(
                url=url,
                pid=pid
            )
        # 请求数据
        try:
            response = requests.get(url=url, params=params, timeout=5)
            return response
        except:
            self.insert_err_url(
                url= urljoin(self.url,'?'+urlencode(self.params)),
                errCode='请求超时',
                pid=pid
            )
            return False

    def insert_err_url(self, url, errCode, pid):
        # 插入请求错误的 url
        self.db[self.coll_name+'_err'].insert(
            {
                'url': url,
                'errCode':errCode,
                'insertTime': time.strftime('%Y-%m-%d %H:%M:%S'),
                'pid':pid
            }
        )

    def insert_ok_url(self, url):
        self.db[self.coll_name+'_ok'].insert({
            'url':url,
            'insertTime':time.strftime('%Y-%m-%d %H:%M:%S'),
        })

    def insert_run_url(self, url, pid):
        self.db[self.coll_name+'_run'].insert({
            'url':url,
            'insertTime': time.strftime('%Y-%m-%d %H:%M:%S'),
            'pid':pid
        })

    def insert_data(self, json_data, isjudge=False):
        # 是否判断重复
        if isjudge:
            cursor = self.db[self.coll_name].find({'id':json_data['id']},{'_id':1})
            if cursor.count() == 0:
                cursor.close()
            else:
                cursor.close()
                return
        self.db[self.coll_name].insert(json_data)

    def has_got(self, url):
        # 判断当前url是否已经获取
        sign = False
        # 判断此url 是否已经读取
        cursor = self.db[self.coll_name + '_ok'].find({'url': url})
        if cursor.count() != 0:
            sign = True
        cursor.close()
        return sign

    def parse(self, response, pid):
        if response is False:
            return

        if response.status_code == 200:
            json_data = response.json()
            if json_data['retcode'] == '000000':
                return json_data
            elif json_data['retcode'] == '100702' or json_data['retcode'] == '100704' or json_data['retcode'] == '100701':
                # api维护升级停用,欠费等问题
                self.insert_err_url(
                    url=response.url,
                    errCode=json_data['retcode'],
                    pid=pid
                )
                time.sleep(60*60*2)
            elif json_data['retcode'] == '100002':
                self.db['OK'].insert({'url':response.url,'insertTime':time.strftime('%Y-%m-%d %H:%M:%S')})
                print('\t\tsearch no result')
            else:
                if json_data['retcode'] == '100703':
                    time.sleep(0.2)
                for i in range(10):
                    if i == 9:
                        self.insert_err_url(
                            url=response.url,
                            errCode='重复请求未果',
                            pid=pid
                        )
                        return False

                    print('\t 尝试重复抓取: ', response.url)
                    json_data = self.get(url=response.url, pid=pid)
                    if json_data.status_code != 200:
                        continue
                    json_data = json_data.json()
                    if json_data['retcode'] == '000000':
                        return json_data

        else:
            self.insert_err_url(
                url=response.url,
                errCode=response.status_code,
                pid=pid
            )
            print('错误的响应码:', response.status_code, 'url:', response.url)
            return False

    # 从数据库获取每个产品的ID
    def get_each_product_id(self):
        cursor = self.db[self.coll_name.replace('Comment','')].find()
        product_id = [cur['id'] for cur in cursor]
        return product_id

    def close(self):
        # 释放数据库连接
        self.client.close()

    def start(self):
        product_id = self.get_each_product_id()
        for pId in product_id:
            print('正在处理:', pId)
            self.params['id'] = pId
            if self.has_got(url=urljoin(self.url, '?' + urlencode(self.params))):
                print('\t已经爬取')
                continue
            try:
                self.params.pop('pageToken')
            except:
                pass
            while True:
                isEnd = False
                response = self.get(url=self.url, params=self.params, pid=pId)
                json_data = self.parse(response, pid=pId)

                if json_data:
                    # print('\t数据写入')
                    for each in json_data['data']:
                        commentTime =time.localtime(each['publishDate'])
                        # 判断时间
                        if commentTime > self.endTime:
                            continue
                        if commentTime < self.startTime:
                            isEnd = True
                            break
                        self.insert_data(each)
                    self.insert_ok_url(url=response.url)

                    if isEnd:
                        break
                    # 判断是否有下一页
                    has_next = json_data['hasNext']
                    if has_next:
                        print('\t获取下一页 pageToken:', json_data['pageToken'])
                        self.params['pageToken'] = json_data['pageToken']
                    else:
                        break
                else:
                    break

    def deal_err(self):
        print('处理出错URL')
        for i in range(3):
            # cursor = self.db[self.coll_name+'_err'].find({},no_cursor_timeout=True)
            cursor = self.db[self.coll_name+'_err'].find({})
            if cursor.count() == 0:
                print('>>>> 处理完毕')
                break
            print('\t第%s次处理'%(i+1))
            for cur in cursor:
                pid = cur['pid']
                print('正在处理:',pid)
                url = re.match(r'(.*?)\?', cur['url']).group(1)
                params = cur['url'][cur['url'].index('?') + 1:].split('&')
                while True:
                    isEnd = False
                    response = self.get(url=url + '?' + '&'.join(params), pid=pid)
                    json_data = self.parse(response, pid=pid)

                    if json_data:
                        # print('\t数据写入')
                        for each in json_data['data']:
                            commentTime = time.localtime(each['publishDate'])
                            # 判断时间
                            if commentTime > self.endTime:
                                continue
                            if commentTime < self.startTime:
                                isEnd = True
                                break
                            self.insert_data(each, isjudge=self.getExcept)
                        self.insert_ok_url(url=response.url)
                        if isEnd:
                            break
                        # 判断是否有下一页
                        has_next = json_data['hasNext']
                        if has_next:
                            print('\t获取下一页 pageToken:', json_data['pageToken'])
                            if len(params) == 3:
                                params[-1] = 'pageToken=%s'%json_data['pageToken']
                            else:
                                params.append('pageToken=%s'%json_data['pageToken'])
                        else:
                            break
                    else:
                        break
                self.db[self.coll_name+'_err'].remove({'_id':cur['_id']})
            cursor.close()

    def __call__(self, *args, **kwargs):
        self.start()
        self.deal_err()
        self.db[self.coll_name+'_run'].remove()
        self.close()


if __name__ == '__main__':
    ffc = FreshFreshComment(getExcept=False)
    ffc()


