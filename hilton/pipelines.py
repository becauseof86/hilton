# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import mysql.connector
import six
from .items import HiltonDetailItem,HiltonAvailabilityItem
from datetime import datetime
from scrapy.mail import MailSender
from random import choice

            
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
        self.connection=mysql.connector.connect(host=self.mysql_host,user=self.mysql_user,password=self.mysql_passwd,database=self.mysql_db,port=self.mysql_port)
        self.cursor=self.connection.cursor()
        
        #获取hilton_codes 格式 {'ZGNGIGI':'Hilton Garden Inn Zhongshan Guzhen',}
        sql='SELECT ctyhocn,name from hotel_detail'
        self.cursor.execute(sql)
        result_tuple=self.cursor.fetchall()
        self.hilton_codes=dict(result_tuple)
        
    def close_spider(self,spider):
        self.cursor.close()
        self.connection.close()
    def make_sql_from_item(self,item,table_name):
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
        return sql
  
    def process_item(self, item, spider):
        if isinstance(item, HiltonAvailabilityItem):
            table_name = 'hotel_availability'
            sql=self.make_sql_from_item(item,table_name)
            self.cursor.execute(sql) #执行SQL
            print '-------------------'+str(self.cursor.rowcount)+'----------------'
            self.connection.commit()# 写入操作
            
            #---------------push notice start----------
            sql='SELECT openid,email,notice_times,max_times FROM weixinuser WHERE openid IN (SELECT openid FROM noticeset WHERE date={} AND ctyhocn={} AND points={} AND (latest_notice_time is NULL OR HOUR(TIMEDIFF(Now(),latest_notice_time))>3))'.format('"'+item['date']+'"','"'+item['ctyhocn']+'"','"'+item['points']+'"')
            self.cursor.execute(sql)
            result_tuple=self.cursor.fetchall()
            print '查询匹配noticeset结果为'
            print result_tuple
            if result_tuple:
                mailsender0=MailSender('smtp.qq.com','95605319@qq.com','95605319','lhytrhunlcahg',465,smtpssl=True)
                mailsender1=MailSender('smtp.exmail.qq.com','bin@data8.info','bin@data8.info','hytrhy',465,smtpssl=True)
                mailsender2=MailSender('smtp.exmail.qq.com','toby@data8.info','toby@data8.info','yhytrhyt',465,smtpssl=True)
                mailsender3=MailSender('smtp.exmail.qq.com','yale@data8.info','yale@data8.info','y6y65',465,smtpssl=True)
                mailsender4=MailSender('smtp.exmail.qq.com','darnell@data8.info','darnell@data8.info','y6htrhy',465,smtpssl=True)
                mailsender5=MailSender('smtp.exmail.qq.com','flora@data8.info','flora@data8.info','y6545y6',465,smtpssl=True)
                mailsender6=MailSender('smtp.exmail.qq.com','lena@data8.info','lena@data8.info','y64y6',465,smtpssl=True)
                mailsender7=MailSender('smtp.exmail.qq.com','martin@data8.info','martin@data8.info','y65yy6',465,smtpssl=True)
                mailsender=choice([mailsender2,mailsender3,mailsender4,mailsender5,mailsender6,mailsender7])
                maillist=[]
                openid_list=[]
                for tup in result_tuple:
                    openid=tup[0]
                    email=tup[1]
                    notice_times=tup[2]
                    max_times=tup[3]
                    print email,notice_times,max_times
                    if notice_times<max_times:#超过最大发送次数 则不会再发送
                        maillist.append(email)
                        openid_list.append(openid)
                if maillist:
                    to=maillist
                    subject=(self.hilton_codes[item['ctyhocn']]+u'有空余基础积分房').encode('utf-8')
                    body=(self.hilton_codes[item['ctyhocn']]+'---'+item['date']+'---'+item['points']+u'---请去预订吧').encode('utf-8')
                    mailsender.send(to,subject,body)
                    openids='('+','.join('"'+openid+'"' for openid in openid_list)+')'
                    #使用mysql自联结 更新用户的总发送邮件次数
                    sql='UPDATE weixinuser AS a,weixinuser AS b SET a.notice_times=b.notice_times+1 WHERE a.openid=b.openid AND a.openid IN {}'.format(openids)
                    #在noticeset表中更新被触发的规则的最近发邮件时间
                    sql2='UPDATE noticeset set latest_notice_time=Now() where date={} AND ctyhocn={} AND points={}'.format('"'+item['date']+'"','"'+item['ctyhocn']+'"','"'+item['points']+'"')
                    self.cursor.execute(sql)
                    self.cursor.execute(sql2)
                    self.connection.commit()# 写入操作                    
            #---------------push notice end------------
            
            
        if isinstance(item, HiltonDetailItem):
            table_name = 'hotel_detail'
            sql=self.make_sql_from_item(item,table_name)
            self.cursor.execute(sql) #执行SQL
            self.connection.commit()# 写入操作

            

