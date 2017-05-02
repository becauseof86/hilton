import scrapy
import re
from ..items import HiltonDetailItem
import json
class HiltonCodeSpider(scrapy.Spider):
    name='hiltoncode'
    start_urls=['http://doubletree3.hilton.com/en_US/dt/ajax/cache/regionHotels.json?regionId=237&subregionId=3038&hotelStatus=null']
    
    def parse(self,response):
        ctyhocns=re.findall(r'ctyhocn":"(.*?)"',response.text)
        names=re.findall(r'name":"(.*?)"',response.text)
        print dict(zip(ctyhocns,names))
        details_dict=json.loads(response.text)["hotels"]
        for detail in details_dict:
            detail_item=HiltonDetailItem()
            for key in detail:
                detail_item[key]=detail[key]
            yield detail_item
        
        
        