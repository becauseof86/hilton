# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class HiltonDetailItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    url = scrapy.Field()
    name = scrapy.Field()
    lat = scrapy.Field()
    lng = scrapy.Field()
    brand = scrapy.Field()
    phone = scrapy.Field()
    address1 = scrapy.Field()
    city = scrapy.Field()
    zip = scrapy.Field()
    country = scrapy.Field()
    status = scrapy.Field()
    pic = scrapy.Field()
    open_date = scrapy.Field()
    reservations = scrapy.Field()
    ctyhocn = scrapy.Field()
    
class HiltonAvailabilityItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    ctyhocn = scrapy.Field()
    date = scrapy.Field()
    update_datetime = scrapy.Field()
    points = scrapy.Field()
