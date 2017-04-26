import scrapy
import re
class HiltonCodeSpider(scrapy.Spider):
    name='hiltoncode'
    start_urls=['http://doubletree3.hilton.com/en_US/dt/ajax/cache/regionHotels.json?regionId=237&subregionId=3038&hotelStatus=null']
    
    def parse(self,response):
        ctyhocns=re.findall(r'ctyhocn":"(.*?)"',response.text)
        names=re.findall(r'name":"(.*?)"',response.text)
        print dict(zip(ctyhocns,names))
        
        