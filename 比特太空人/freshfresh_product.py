'''
    两鲜产品
'''
import requests
import time
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

class FreshFreshProduct:
    def __init__(self,dbname='Fresh', getExcept=False):
        self.url = 'http://120.76.205.241:8000/product/freshfresh'
        self.params = {
            'cityid':'',
            'apikey':'iyRRRaIDWDou6yiDZqAOL9WU03ijEXJfYVxcfvihYOi8BzYE31FoOLovWgHCbIeg',
            'catid':'',
        }
        self.client = MongoClient(uri)
        self.db = self.client[dbname]
        # 生成集合名
        tm = time.localtime()
        # 根据当前抓取日期生成集合名称
        self.coll_name = 'freshFresh_%s-%s'%(tm.tm_year,tm.tm_mon)
        # 程序是否异常终止
        self.getExcept = getExcept
        # 上次正爬取的url转入错误集合
        cursor = self.db[self.coll_name+'_run'].find()
        for cur in cursor:
            cur['errCode'] = '异常终止'
            self.db[self.coll_name+'_err'].insert(cur)
        cursor.close()
        self.db[self.coll_name + '_run'].remove()

    def get(self, url, params=None, city=None,cat=None):
        self.db[self.coll_name+'_run'].remove()
        if params:
            self.insert_run_url(
                url=url + '?' + urlencode(self.params),
                city=city,
                cat=cat
            )
        else:
            self.insert_run_url(
                url=url,
                city=city,
                cat=cat
            )
        # 请求数据
        try:
            response = requests.get(url=url, params=params, timeout=5)
            return response
        except:
            self.insert_err_url(
                url= urljoin(self.url,'?'+urlencode(self.params)),
                errCode='请求超时',
                city=city,
                cat=cat
            )
            return False

    def insert_err_url(self, url, errCode, city, cat):
        # 插入请求错误的 url
        self.db[self.coll_name+'_err'].insert(
            {
                'url': url,
                'errCode':errCode,
                'insertTime': time.strftime('%Y-%m-%d %H:%M:%S'),
                'city': city,
                'cat': cat,
            }
        )

    def insert_ok_url(self, url):
        self.db[self.coll_name+'_ok'].insert({
            'url':url,
            'insertTime':time.strftime('%Y-%m-%d %H:%M:%S'),
        })

    def insert_run_url(self, url, city, cat):
        self.db[self.coll_name+'_run'].insert({
            'url':url,
            'city':city,
            'cat':cat,
            'insertTime': time.strftime('%Y-%m-%d %H:%M:%S'),
        })

    def insert_data(self, json_data, city, cat, isjudge=False):
        # 是否判断重复
        if isjudge:
            cursor = self.db[self.coll_name].find({'id':json_data['id']},{'_id':1})
            if cursor.count() == 0:
                cursor.close()
            else:
                cursor.close()
                return

        json_data['city'] = city
        json_data['cat'] = cat
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

    def parse(self, response, city=None, cat=None):
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
                    city=city,
                    cat=cat
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
                            city=city,
                            cat=cat
                        )
                        return False

                    print('\t 尝试重复抓取: ', response.url)
                    json_data = self.get(url=response.url)
                    if json_data.status_code != 200:
                        continue
                    json_data = json_data.json()
                    if json_data['retcode'] == '000000':
                        return json_data

        else:
            self.insert_err_url(
                url=response.url,
                errCode=response.status_code,
                city=city,
                cat=cat
            )
            print('错误的响应码:', response.status_code, 'url:', response.url)
            return False

    # 根据数据库获取cityId与catId
    def get_each_city_cat(self):
        # 处理每个城市的种类
        city_cats = []
        cursor = self.db['freshFreshCatId'].find()
        catIds = [{'id':i['catId'],'name':i['catName']} for i in cursor]
        cursor = self.db['freshFreshCityId'].find()
        for cur in cursor:
            city_cats.append({
                'cityId':cur['cityId'],
                'cityName':cur['cityName'],
                'cats':catIds
            })
        return city_cats

    def close(self):
        # 释放数据库连接
        self.client.close()

    def start(self):
        city_cats = self.get_each_city_cat()
        for city_cat in city_cats:
            print('正在处理:', city_cat['cityName'])
            self.params['cityid'] = city_cat['cityId']
            for cat in city_cat['cats']:
                print('>>>>', cat['name'])
                self.params['catid'] = cat['id']
                if self.has_got(url=urljoin(self.url, '?' + urlencode(self.params))):
                    print('\t已经爬取')
                    continue
                try:
                    self.params.pop('pageToken')
                except:
                    pass
                while True:
                    response = self.get(url=self.url, params=self.params, city=city_cat['cityName'], cat=cat['name'])
                    json_data = self.parse(response, city=city_cat['cityName'], cat=cat['name'])

                    if json_data:
                        # print('\t数据写入')
                        for each in json_data['data']:
                            self.insert_data(each, city=city_cat['cityName'], cat=cat['name'])
                        self.insert_ok_url(url=response.url)
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
            cursor = self.db[self.coll_name+'_err'].find({},no_cursor_timeout=True)
            if cursor.count() == 0:
                print('>>>> 处理完毕')
                break
            print('\t第%s次处理'%(i+1))
            for cur in cursor:
                city = cur['city']
                cat = cur['cat']
                url = re.match(r'(.*?)\?', cur['url']).group(1)
                params = cur['url'][cur['url'].index('?') + 1:].split('&')
                print('处理 >>>> ',city,cat)
                while True:
                    response = self.get(url=url + '?' + '&'.join(params), city=city, cat=cat)
                    json_data = self.parse(response, city=city, cat=cat)

                    if json_data:
                        # print('\t数据写入')
                        for each in json_data['data']:
                            self.insert_data(each, city=city, cat=cat,isjudge=self.getExcept)
                        self.insert_ok_url(url=response.url)
                        # 判断是否有下一页
                        has_next = json_data['hasNext']
                        if has_next:
                            print('\t获取下一页 pageToken:', json_data['pageToken'])
                            if len(params) == 4:
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
    ffp = FreshFreshProduct(getExcept=False)
    ffp()


