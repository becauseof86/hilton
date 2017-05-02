import scrapy
import datetime
from ..items import HiltonAvailabilityItem
class HiltonSpider(scrapy.Spider):
    name='hilton'
    hilton_codes={'CTULODI':r'DoubleTree by Hilton Hotel Chengdu Longquanyi',
'CKGNADI':r'DoubleTree by Hilton Hotel Chongqing - Nan'an',
'BJSDTDI':r'DoubleTree by Hilton Hotel Beijing',
}
    start_urls=['https://secure3.hilton.com/en_US/dt/reservation/book.htm?internalDeepLinking=true?inputModule=HOTEL_SEARCH&ctyhocn='+hilton_code for hilton_code in hilton_codes]
    today=datetime.date.today()
    seven_days=datetime.timedelta(days=7)
    eight_days=datetime.timedelta(days=8)
    def start_requests(self):
        
        for url in self.start_urls:
            room_availability={
            'room':{},
            'name':self.hilton_codes[url[-7:]]
            }
            yield scrapy.Request(url,self.parse,dont_filter=True,meta={'ctyhocn':url[-7:],'date':self.today,'room_availability':room_availability})
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
        yield scrapy.FormRequest.from_response(response,formdata=formdata,callback=self.after_query,dont_filter=True,meta={'ctyhocn':response.meta['ctyhocn'],'date':new_date_for_next_request,'room_availability':response.meta['room_availability']})

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
            yield scrapy.FormRequest.from_response(response,formdata=formdata,callback=self.after_query,dont_filter=True,meta={'ctyhocn':response.meta['ctyhocn'],'date':new_date_for_next_request,'room_availability':response.meta['room_availability']})
        else:
            response.meta['room_availability']['update_time']=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            yield response.meta['room_availability']
            















