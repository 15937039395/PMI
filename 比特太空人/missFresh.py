'''
    每日生鲜
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

class MissFresh:
    '''
        只传cityid 获取当前城市的所有分类
        cityid+catid 获取当前城市,当前分类下的检索结果
    '''
    def __init__(self,dbname='Fresh', getExcept=False):
        self.url = 'http://120.76.205.241:8000/product/missfresh'
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
        self.coll_name = 'missFresh_%s-%s'%(tm.tm_year,tm.tm_mon)
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
            cursor = self.db[self.coll_name].find({'id':json_data['id']})
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

    def clear_catId(self):
        self.db['missFreshCatId'].remove()

    def parse_catid(self, cities):
        err_list = []
        for city in cities:
            self.params['cityid'] = city['cityId']
            try:
                response = requests.get(url=self.url, params=self.params, timeout=5)
            except:
                err_list.append(city)
                continue
            if response.status_code == 200:
                json_data = response.json()
                if json_data['retcode'] == '100002':
                    json_data['data'] = []

                if json_data['retcode'] != '100002' and json_data['retcode'] != '000000':
                    err_list.append(city)
                    continue

                self.db['missFreshCatId'].insert({
                    'cityName': city['cityName'],
                    'cityId': city['cityId'],
                    'cats': json_data['data']
                })
            else:
                err_list.append(city)
        return err_list

    # 通过比特接口获取每个城市catId
    def get_catid(self):
        # 每月抓取前 获取商品分类
        cities = self.db['missFreshCityId'].find()
        err_list = self.parse_catid(cities=cities)
        print('处理出错city:')
        for i in range(3):
            if err_list:
                print('\t第%s次处理错误'%(i+1))
                err_list = self.parse_catid(err_list)
            else:
                print('\t错误处理完毕')
                break
        if err_list:
            print('部分数据获取失败,请手动添加 并注释代码 323行:')
            for each in err_list:
                print('\t',each)
            exit(-1)

    # 根据数据库中的catId,筛选所需的catId
    def get_each_city_cat(self):
        # 处理每个城市的种类
        city_cats = []
        cursor = self.db['missFreshCatId'].find()
        for cur in cursor:
            cats = []
            for cat in cur['cats']:
                if '水果' == cat['name'] or '水产' == cat['name'] or '肉蛋' == cat['name'] or '蔬菜' == cat['name']:
                    cats.append(cat)
            if cats:
                city_cats.append({
                    'cityName':cur['cityName'],
                    'cityId':cur['cityId'],
                    'cats':cats
                })
        cursor.close()
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
                self.db[self.coll_name + '_err'].remove({'_id': cur['_id']})
            cursor.close()

    def __call__(self, *args, **kwargs):
        # 清空 catid 重新获取
        self.clear_catId()
        # 获取每个城市的catid
        self.get_catid()
        self.start()
        self.deal_err()
        self.db[self.coll_name+'_run'].remove()
        self.close()
        pass


if __name__ == '__main__':
    missFresh = MissFresh(getExcept=False)
    missFresh()

