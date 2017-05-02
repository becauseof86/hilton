# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import MySQLdb
import six
from .items import HiltonDetailItem,HiltonAvailabilityItem
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
            
class HiltonPipelineMysql(object):
    def __init__(self,mysql_host,mysql_user,mysql_passwd,mysql_db,mysql_port):
        self.mysql_host=mysql_host
        self.mysql_user=mysql_user
        self.mysql_passwd=mysql_passwd
        self.mysql_db=mysql_db
        self.mysql_port=mysql_port
       
    @classmethod
    def from_crawler(cls,crawler):
        return cls(crawler.settings.get('MYSQL_HOST'),crawler.settings.get('MYSQL_USER'),crawler.settings.get('MYSQL_PASSWD'),crawler.settings.get('MYSQL_DB'),crawler.settings.get('MYSQL_PORT'))
    
    def open_spider(self,spider):
        self.connection=MySQLdb.connect(self.mysql_host,self.mysql_user,self.mysql_passwd,self.mysql_db,self.mysql_port)
        self.cursor=self.connection.cursor()
    def process_item(self, item, spider):
        if isinstance(item, HiltonAvailabilityItem):
            table_name = 'hotel_availability'
            col_str = ''
            row_str = ''
            for key in item.keys():
                col_str = col_str + " " + key + ","
                if isinstance(item[key],bool) or isinstance(item[key],float):
                    row_str = "{}'{}',".format(row_str, item[key])
                else:
                    row_str = "{}'{}',".format(row_str, item[key] if "'" not in item[key] else item[key].replace("'", "\\'"))
                sql = "insert INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE ".format(table_name, col_str[1:-1], row_str[:-1])
            for (key, value) in six.iteritems(item):
                if isinstance(value,bool) or isinstance(value,float):
                    sql += "{} = '{}', ".format(key, value)
                else:
                    sql += "{} = '{}', ".format(key, value if "'" not in value else value.replace("'", "\\'"))
            sql = sql[:-2]
            print sql
            res=self.cursor.execute(sql) #执行SQL
            print res
            self.connection.commit()# 写入操作
            
        if isinstance(item, HiltonDetailItem):
            table_name = 'hotel_detail'
            col_str = ''
            row_str = ''
            for key in item.keys():
                col_str = col_str + " " + key + ","
                if isinstance(item[key],bool) or isinstance(item[key],float):
                    row_str = "{}'{}',".format(row_str, item[key])
                else:
                    row_str = "{}'{}',".format(row_str, item[key] if "'" not in item[key] else item[key].replace("'", "\\'"))
                sql = "insert INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE ".format(table_name, col_str[1:-1], row_str[:-1])
            for (key, value) in six.iteritems(item):
                if isinstance(value,bool) or isinstance(value,float):
                    sql += "{} = '{}', ".format(key, value)
                else:
                    sql += "{} = '{}', ".format(key, value if "'" not in value else value.replace("'", "\\'"))
            sql = sql[:-2]
            print sql
            self.cursor.execute(sql) #执行SQL
            self.connection.commit()# 写入操作

            

