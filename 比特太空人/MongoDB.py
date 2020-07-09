import pymongo
import datetime
import json


class MyMongoDB():
    dbUrl = ''
    client = ''
    db = ''
    coll = ''

    def __init__(self, dbUrl):
        self.dbUrl = dbUrl

    def conn(self):
        self.client = pymongo.MongoClient(self.dbUrl)
        return self

    def selectDB(self, dbName):
        self.db = self.client[dbName]

    def selectColl(self, collName):
        self.coll = self.db[collName]

    def getCityIdList(self):
        self.selectColl('CityId')
        data = self.coll.find_one({}, {'_id': 0})
        if data:
            cityid = str(data['cityid'])
            splitCityId = cityid.split('-')
            return splitCityId

    def insert(self, data, date,week):
        self.selectColl('DianpingShop_' + date)
        data['createTime'] = datetime.datetime.now()
        id = data['id']
        data['week'] = week
        data['comment'] = False
        find = self.coll.find_one({'id': id})
        if not find:
            if self.coll.insert(data):
                return 0  # 插入成功
            else:
                return 1  # 插入失败
        else:
            return 2  # 重复id


    def insertDianping(self, data, commentCollName,shopInfo,date,week):
        shopid=shopInfo['shopid']
        lv1Name=shopInfo['lv1Name']
        lv2Name=shopInfo['lv2Name']
        title=shopInfo['title']
        self.selectColl('DianpingComment_' + commentCollName)
        data['createTime'] = datetime.datetime.now()
        id = data['id']
        data['week'] = week
        data['shopName']=title
        data['lv1Name']=lv1Name
        data['lv2Name']=lv2Name
        find = self.coll.find_one({'id': id,'week':week})
        if not find:
            if self.coll.insert(data):
                return 0  # 插入成功
            else:
                return 1  # 插入失败
        else:
            return 2  # 重复id


    def setLogPage(self, date, parameter, pageToken):
        self.selectColl('DianpingShopLog_' + date)
        find = self.coll.find_one({'kw': parameter['kw'], 'week': parameter['week']})
        if find:
            if 'createTime' not in dict(find).keys():
                parameter['createTime'] = datetime.datetime.now()
            parameter['pageToken'] = pageToken
            parameter['updateTime'] = datetime.datetime.now()
            self.coll.update_one({'kw': parameter['kw'], 'week': parameter['week']}, {'$set':parameter})

    def getLogPage(self, date, parameter):
        page = 0
        self.selectColl('DianpingShopLog_' + date)
        find = self.coll.find_one({'kw': parameter['kw'], 'week': parameter['week']})
        if find:
            if 'pageToken' in dict(find).keys():  # 如果data有pageToken字段,下面同理
                page = find['pageToken']
            if page == None:
                page = 0
                return page
            if int(page) < 0:
                page = 0
                return page
            else:
                return page
        else:
            parameter['pageToken'] = 0
            parameter['createTime'] = datetime.datetime.now()
            parameter['updateTime'] = datetime.datetime.now()
            self.coll.insert(parameter)
            return page

    def getCityId(self, date, parameter):
        cityid = 1
        self.selectColl('DianpingShopLog_' + date)
        find = self.coll.find_one({'kw': parameter['kw'], 'week': parameter['week']})
        if find:
            cityid = find['cityid']
            return cityid
        else:
            parameter['cityid'] = 1
            parameter['pageToken'] = 0
            parameter['createTime'] = datetime.datetime.now()
            parameter['updateTime'] = datetime.datetime.now()
            self.coll.insert(parameter)
            return cityid

    def resetPageToken(self, date, parameter):
        self.selectColl('DianpingShopLog_' + date)
        find = self.coll.find_one({'kw': parameter['kw'], 'week': parameter['week']})
        if find:
            if 'pageToken' in dict(find).keys():  # 如果data有pageToken字段,下面同理
                page = find['pageToken']
                if page!= None:
                    parameter['pageToken'] = page
                else:
                    parameter['pageToken'] = 0

            if 'cityid' in dict(find).keys():  # 如果data有pageToken字段,下面同理
                cityid = find['cityid']
                if cityid != None:
                    parameter['cityid'] = cityid
                else:
                    parameter['cityid'] = 1

            if 'createTime' in dict(find).keys():  # 如果data有pageToken字段,下面同理
                createTime = find['createTime']
                if createTime != None:
                    parameter['createTime'] = createTime
                else:
                    parameter['createTime'] = datetime.datetime.now()

            if 'updateTime' in dict(find).keys():  # 如果data有pageToken字段,下面同理
                updateTime = find['updateTime']
                if cityid != None:
                    parameter['updateTime'] = updateTime
                else:
                    parameter['updateTime'] = datetime.datetime.now()

        self.coll.update({'kw': parameter['kw'], 'week': parameter['week']}, parameter)

    def getDianpingShop(self,date,week):
        self.selectColl('DianpingShop_'+date)
        shopid = self.coll.find_one({'week':week,'comment':False}, {'_id':0,'id': 1})
        if shopid:
           if 'id' in dict(shopid).keys():
             return shopid['id']
        else:
            print("未查到comment=False")
            return


    def setDianpingShopCommetBool(self, date, shopid,week,isNone=False):
        self.selectColl('DianpingShop_' + date)
        if isNone==True:
            self.coll.update_one({'id': shopid, 'week': week}, {'$set': {"comment": True}})
        else:
            self.coll.update_one({'id':shopid,'week':week},{'$set':{"comment":True}})
