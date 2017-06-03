# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.downloadermiddlewares.retry import RetryMiddleware
import logging
from scrapy.exceptions import CloseSpider
import random

logger = logging.getLogger(__name__)

class ChangePostDataMiddleware(object):
    def process_request(self,request,spider):  #hilton不守规范 把post数据中的%2B 替换成+ 否则服务器返回错误
        body=request.body
        if body.find('%2B')>-1:
            newbody=body.replace('%2B','+')
            request=request.replace(body=newbody)
            return request


class HiltonSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
        
class RetryMiddlewareNew(RetryMiddleware):
    def __init__(self, settings):
        super(RetryMiddlewareNew,self).__init__(settings)
        self.mysql_host=settings.get('MYSQL_HOST')
        self.mysql_user=settings.get('MYSQL_USER')
        self.mysql_passwd=settings.get('MYSQL_PASSWD')
        self.mysql_db='proxy'
        self.mysql_port=settings.get('MYSQL_PORT')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) \
                and not request.meta.get('dont_retry', False):
            proxy=request.meta.get('proxy',None)
            proxy_pool=request.meta.get('proxy_pool',None)
            print proxy_pool
            if not proxy_pool:
                raise CloseSpider('no more proxy ip')
            if proxy:
                if proxy not in proxy_pool:
                    print 'pass'
                    pass
                else:
                    if proxy_pool[proxy]>4: # failed for more than 4 times
                        try:
                            del proxy_pool[proxy]
                            print '-------------------------------------'
                            print 'too many failed times of this ip,delete',proxy
                            print '-------------------------------------' 
                        except Exception,e:
                            print e    
                    else:
                        try:
                            proxy_pool[proxy]=proxy_pool[proxy]+1 #add failed times
                            print '-------------------------------------'
                            print 'failed times increase',proxy
                            print '-------------------------------------' 
                        except Exception,e:
                            print e
                             
            return self._retry(request, exception, spider)
    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1

        if retries <= self.max_retry_times:
            logger.debug("Retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra={'spider': spider})
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            proxy_pool=request.meta.get('proxy_pool',None)
            if not proxy_pool:
                raise CloseSpider('no more proxy ip')
            else:
                try:                       
                    retryreq.meta['proxy']=random.choice(proxy_pool.keys())
                    print retryreq.meta['proxy']
                except Exception,e:
                    print e
                print '-------------------------------------'
                print 'add proxy',retryreq.meta['proxy']
                print '-------------------------------------'

            
            retryreq.dont_filter = True
            retryreq.priority = request.priority + self.priority_adjust
            return retryreq
        else:
            logger.debug("Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra={'spider': spider})