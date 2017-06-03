#coding:utf-8
import scrapy
import datetime
from ..items import HiltonAvailabilityItem
from scrapy.exceptions import CloseSpider
import random
import mysql.connector

class HiltonSpider(scrapy.Spider):
    name='hilton'
    custom_settings={
    'RETRY_ENABLED':True,
    'RETRY_TIMES':20,
    'RETRY_HTTP_CODES':[500, 502, 503, 504, 408,403,404],
    'DOWNLOAD_DELAY' : 0.1,
    'CONCURRENT_REQUESTS' :32,
    'CONCURRENT_REQUESTS_PER_DOMAIN' : 32
    }
    today=datetime.date.today()
    seven_days=datetime.timedelta(days=7)
    eight_days=datetime.timedelta(days=8)
    
    def __init__(self,settings):
        self.mysql_host=settings.get('MYSQL_HOST')
        self.mysql_user=settings.get('MYSQL_USER')
        self.mysql_passwd=settings.get('MYSQL_PASSWD')
        self.mysql_db=settings.get('MYSQL_DB')
        self.mysql_port=settings.get('MYSQL_PORT')
        self.connection=mysql.connect(host=self.mysql_host,user=self.mysql_user,password=self.mysql_passwd,database=self.mysql_db,port=self.mysql_port)
        self.cursor=self.connection.cursor()
        super(HiltonSpider,self).__init__()
    @classmethod
    def from_crawler(cls,crawler):
        settings=crawler.settings
        spider=cls(settings)
        crawler.signals.connect(spider.close_spider, signal=signals.spider_closed)
        return spider
    
    def start_requests(self):
        sql='SELECT * FROM validproxy'
        self.cursor.execute(sql)
        result_tuple=self.cursor.fetchall()
        if result_tuple:
            proxy_pool=dict((tuple[0],0) for tuple in result_tuple)
        else:
            raise CloseSpider('no proxy ip in table validproxy')

        sql='select ctyhocn,name from hotel_detail where status="OPEN" and country="China" and ((open_date is not NULL and DateDiff(Now(),str_to_date(open_date,"%d %M %Y"))>=0) or open_date is NULL)'
        self.cursor.execute(sql)
        result_tuple=self.cursor.fetchall()
        if result_tuple:
            hilton_codes=dict(result_tuple)
            if hilton_codes.get('WUXTJDI',None):
                del hilton_codes['WUXTJDI']  #DELETE TAIZHOU HILTON 
        else:
            raise CloseSpider('no hiltoncode in hotel_detail')
        #print hilton_codes
        self.start_urls=['https://secure3.hilton.com/en_US/dt/reservation/book.htm?internalDeepLinking=true?inputModule=HOTEL_SEARCH&ctyhocn='+hilton_code for hilton_code in hilton_codes]
       
        for i,url in enumerate(self.start_urls):
            room_availability={
            'room':{},
            'name':hilton_codes[url[-7:]]
            }
            #meta里边先随机从proxy_pool里挑一个proxy使用,顺便把整个proxy_pool往下传 方便后面的middleware遇到代理不能使用的时候继续随机挑选其他代理
            yield scrapy.Request(url,self.parse,dont_filter=True,meta={'proxy':random.choice(proxy_pool.keys()),'proxy_pool':proxy_pool,'ctyhocn':url[-7:],'date':self.today,'room_availability':room_availability,'cookiejar':i}) #'cookiejar':i 每一个起始request都有各自的cookie,不能互相混淆
    def parse(self,response):
        date=response.meta['date']
        arrival_date=(date+self.seven_days).strftime('%d+%b+%Y')
        departure_date=(date+self.eight_days).strftime('%d+%b+%Y')
        formdata={
        'arrivalDate':arrival_date,
        'departureDate':departure_date,
        'flexibleDates':'true',
        '_flexibleDates':'on',
        'rewardBooking':'true',
        '_rewardBooking':'on'
        }
        new_date_for_next_request=date+self.seven_days+self.eight_days           
        yield scrapy.FormRequest.from_response(response,formdata=formdata,callback=self.after_query,dont_filter=True,meta={'proxy':response.meta.get('proxy',''),'proxy_pool':response.meta.get('proxy_pool',''),'ctyhocn':response.meta['ctyhocn'],'date':new_date_for_next_request,'room_availability':response.meta['room_availability'],'cookiejar':response.meta['cookiejar']})

    def after_query(self,response):
        item=HiltonAvailabilityItem()
        if not response.xpath("//table[@class='tblAvailCal']"):
            print 'posting to e1s1 page did not get the right page'
            return
        trs=response.selector.xpath("//tbody[@class='tbodyAvailCal']/tr")
        for tr in trs:
            #tr.xpath("//td/input/@value") is not right
            checkin_date=tr.xpath("td/input/@value").extract_first()[:10]
            
            redeemed_points=tr.xpath("td[@class='priceOrAvailability']/strong[@class='priceSpanPricePerNight']/text()").extract_first()
            if redeemed_points is None:
                redeemed_points='sold out'
            if checkin_date:
                standard_datetime=datetime.datetime.strptime(checkin_date,'%m/%d/%Y')
                checkin_date_mysql=standard_datetime.strftime('%Y-%m-%d')
                print checkin_date_mysql+'-----'+redeemed_points
                item['date']=checkin_date_mysql
                item['update_datetime']=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                item['points']=redeemed_points
                item['ctyhocn']=response.meta['ctyhocn']
                yield item   #for mysql to store
                response.meta['room_availability']['room'].update({checkin_date:redeemed_points})
            else:
                print 'the page is wrong'
                return 
        
        date=response.meta['date']
        if (date-self.today).days<46:  #query points room availability for four times
            arrival_date=(date+self.seven_days).strftime('%d+%b+%Y')
            departure_date=(date+self.eight_days).strftime('%d+%b+%Y')
            formdata={
            'flexibleDates':'true',
            'arrivalDate':arrival_date,
            'departureDate':departure_date,
            '_eventId_changeDates':'Update',
            'selectedRequestedRate':'POINTS_STANDARD_REDEMPTION'
            }
            new_date_for_next_request=date+self.seven_days+self.eight_days
            yield scrapy.FormRequest.from_response(response,formdata=formdata,callback=self.after_query,dont_filter=True,meta={'proxy':response.meta.get('proxy',''),'proxy_pool':response.meta.get('proxy_pool',''),'ctyhocn':response.meta['ctyhocn'],'date':new_date_for_next_request,'room_availability':response.meta['room_availability'],'cookiejar':response.meta['cookiejar']})
        else:
            response.meta['room_availability']['update_time']=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            yield response.meta['room_availability']
            
    def close_spider(self):
        self.cursor.close()
        self.connection.close()
        print '-----------------'
        print u'closing database'
        print '-----------------' 
