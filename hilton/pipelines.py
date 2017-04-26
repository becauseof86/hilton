# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo

class HiltonPipeline(object):
    collection_name='hilton'
    def __init__(self,mongo_uri,mongo_db):
        self.mongo_uri=mongo_uri
        self.mongo_db=mongo_db

    @classmethod
    def from_crawler(cls,crawler):
        return cls(crawler.settings.get('MONGO_URI'),crawler.settings.get('MONGO_DATABASE','hilton'))
   
    def open_spider(self,spider):
        self.client=pymongo.MongoClient(self.mongo_uri)
        self.db=self.client[self.mongo_db]

    def close_spider(self,spider):
        self.client.close()

    def process_item(self, item, spider):
        res=self.db[self.collection_name].insert(item)
        print res
        if item['name']:
            return item['name']
        else:
            print 'item failed'
