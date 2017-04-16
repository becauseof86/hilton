import scrapy
import datetime

class HiltonSpider(scrapy.Spider):
    name='hilton'
    start_urls=['https://secure3.hilton.com/en_US/dt/reservation/book.htm?internalDeepLinking=true?inputModule=HOTEL_SEARCH&ctyhocn=NKGWUDI']

    def parse(self,response):
        today=datetime.date.today()
        sevendays=datetime.timedelta(days=7)
        eightdays=datetime.timedelta(days=8)
        arrivaldate=(today+sevendays).strftime('%d+%b+%Y')
        departuredate=(today+eightdays).strftime('%d+%b+%Y')
        formdata={
        'arrivalDate':arrivaldate,
        'departureDate':departuredate,
        'flexibleDates':'true',
        '_flexibleDates':'on',
        'rewardBooking':'true',
        '_rewardBooking':'on'
        }

        yield scrapy.FormRequest.from_response(response,formdata=formdata,callback=self.after_query)

    def after_query(self,response):
        print response.text
        trs=response.selector.xpath("//tbody[@class='tbodyAvailCal']/tr")
        for tr in trs:
            checkindate=tr.xpath("//td/input/@value").extract_first()
            checkinpoints=tr.xpath("//strong[@class='priceSpanPricePerNight']/text()").extract_first()
            print checkindate+'-----'+checkinpoints















