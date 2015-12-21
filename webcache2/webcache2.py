#! /usr/bin/env python
# -*- encoding: utf-8 -*-

import logging

# scrapping
import socket
socket.setdefaulttimeout(120)
import urllib2
# import urllib
import urlparse
import httplib
from BeautifulSoup import BeautifulSoup
import simplejson

# time
import datetime
import time

# gzip
import zlib

# cookies
import os
import cookielib

# db
# import bson
import pymongo

from pprint import pprint

# db.json.ensure_index([('url',1)])
# db.json.ensure_index([('date',-1)])
# db.page.ensure_index([('url',1)])
# db.page.ensure_index([('date',-1)])
# db.log.ensure_index([('url',1)])
# db.log.ensure_index([('date',-1)])

logger = logging.getLogger('webcache')


class WebcacheFailure(Exception):
    pass


class TwitterRateLimit(Exception):

    def __init__(self, wait_until):
        self.wait_until = wait_until

    def __str__(self):
        return 'wait until %s' % self.wait_until


class w2(object):

    def __init__(self, host='localhost', proxy=None, dbname='webcache'):
        self.host = host
        self.proxy = proxy
        self.dbname = dbname
        self.connection = pymongo.Connection(self.host)
        self.db = self.connection[self.dbname]

    def __del__(self):
        self.connection.close()

    @staticmethod
    def replace_in_keys(source_dict, to_replace, replace_by):
        if isinstance(source_dict, dict):
            ret_dict = dict(source_dict)
            for old_key in ret_dict.keys():
                new_key = old_key.replace(to_replace, replace_by)
                ret_dict[new_key] = w2.replace_in_keys(ret_dict[old_key], to_replace, replace_by)
                if old_key != new_key:
                    del ret_dict[old_key]
            return ret_dict
        if isinstance(source_dict, list):
            ret_list = []
            for elmt in source_dict:
                ret_list.append(w2.replace_in_keys(elmt, to_replace, replace_by))
            return ret_list
        else:
            return source_dict

    @staticmethod
    def get_real_url(url):
        ''' Returns real url (http anything except 3xx/5xx)
        '''
        linkUrl = url

        # 10 http header requests only
        status = 0
        for i in range(1, 10):
            try:
                # print 'trying... %s' % linkUrl
                # Separate host from path
                urlInParts = urlparse.urlsplit(linkUrl)
                host = urlInParts[1]
                if host == '':
                    host = oldhost
                    linkUrl = ''.join(['http://%s' % host, linkUrl])
                oldhost = host
                if urlInParts[3]:
                    path = '%s?%s' % (urlInParts[2], urlInParts[3])
                else:
                    path = urlInParts[2]
                # HEAD request
                connection = httplib.HTTPConnection(host)
                header = {
                    'User-agent': 'Mozilla/5.0 (X11; U; Linux i686; de; rv:1.8) Gecko/20051128 SUSE/1.5-0.1 Firefox/1.5',
                    'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
                    'Accept-Language': 'de-de,de;q=0.8,en-us;q=0.5,en;q=0.3',
                    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
                    'Keep-Alive': '30',
                    'Connection': 'keep-alive',
                }
                connection.request("HEAD", path, None, header)
                responseOb = connection.getresponse()
                status = responseOb.status
                # print status
                if (status / 100 == 3):
                    linkUrl = responseOb.getheader('location')
                elif (status / 100 == 5):
                    pass
                else:
                    break
            except TypeError:
                pass
            #~ except socket.error:
                #~ pass
            #~ except urllib2.HTTPError, e:
                #~ pass
            #~ except urllib2.URLError:
                #~ pass
            #~ except:
                #~ pass
        # workaround for lexpansion anchor bug
        #~ if 'lexpansion.lexpress.fr' in linkUrl and 'xtor' in linkUrl:
        if 'xtor' in linkUrl:
            linkUrl = linkUrl.split('#')[0]
        return linkUrl

    def get_canonical(self, url):
        oldurl = url
        domain = '/'.join(url.split('/')[0:3])
        p = self.get_page(url, tries=[0])
        if p is not None and 'page' in p and p['page'] is not None:
            b = BeautifulSoup(p['page'])
            if b.find('link', {'rel': 'canonical'}) is not None:
                url = b.find('link', {'rel': 'canonical'})['href']
            if b.find('meta', {'property': 'og:url'}) is not None:
                url = b.find('meta', {'property': 'og:url'}).get('content', url)
            # rue89bug : empty canonical
            if url == '':
                url = oldurl
            # relative path bug
            if '://' not in url:
                url = ''.join([domain, url])
            # nouvelobs bug : canonical can be:
            # http://tempsreel.nouvelobs.com/http://sports.nouvelobs.com/20110504.OBS2391/quotas-discriminatoires-la-direction-de-la-fff-aurait-ete-informee.html
            if url.count('http://') > 1:
                url = oldurl
        return url

    def get_html_from_url(self, url, tries=[1, 2, 4, 8], format='soup', twitter_bearer_token=None, can_wait=True, save_cookies=False):
        if twitter_bearer_token:
            tries = [0, 0, 0]
        for sleep_time in tries:
            try:
                # Proxy
                if self.proxy is not None:
                    proxy_support = urllib2.ProxyHandler({'http': self.proxy})
                    opener = urllib2.build_opener(proxy_support, urllib2.HTTPHandler)
                    urllib2.install_opener(opener)
                # Headers
                headers = {}
                headers['User-Agent'] = 'Mozilla/5.0 (Windows; U; Windows NT 6.1; fr; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8'
                headers['Accept-Language'] = 'fr,fr-fr;q=0.8,en-us;q=0.5,en;q=0.3'
                headers['Accept-Encoding'] = 'identity'
                headers['Accept-Charset'] = 'ISO-8859-1,utf-8;q=0.7,*;q=0.7'
                headers['Keep-Alive'] = '115'
                headers['Connection'] = 'keep-alive'
                headers['Cache-Control'] = 'max-age=0'
                if twitter_bearer_token:
                    headers['Authorization'] = 'Bearer %s' % twitter_bearer_token
                # Cookies
                cj = cookielib.LWPCookieJar()

                if save_cookies:
                    if os.path.isfile('cookiefile'):
                        cj.load('cookiefile', ignore_discard=True)

                opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
                urllib2.install_opener(opener)

                req = urllib2.Request(url, None, headers)
                res = urllib2.urlopen(req)

                if save_cookies:
                    cj.save('cookiefile', ignore_discard=True)

                # Read now !
                data = res.read()
                # Gzip
                isGZipped = res.headers.get('content-encoding', '').find('gzip') >= 0
                if isGZipped:
                    gzip_d = zlib.decompressobj(16 + zlib.MAX_WBITS)  # this magic number can be inferred from the structure of a gzip file
                    data = gzip_d.decompress(data)
                #~ info = res.info()
                if format == 'soup':
                    page = BeautifulSoup(data)
                    return page.prettify(), None
                elif format == 'json':
                    json = simplejson.loads(data)
                    return json, None
                #~ return data
            except Exception as e:
                error = {
                    'level': 'error',
                    'type': e.__class__,
                    'reason': e.message,
                    'http_code': getattr(e, 'code', None),
                    'www-authenticate': getattr(e, 'headers', {}).get('WWW-Authenticate', None),
                    'url': url,
                    'date': datetime.datetime.utcnow()
                }
                if twitter_bearer_token and isinstance(e, urllib2.HTTPError):
                    if int(e.code) == 429:  # too many requests (Twitter)
                        for h in e.info().headers:
                            if h.startswith('x-rate-limit-reset'):
                                reset_time = h.split(':')[1].strip()
                                reset_time = int(reset_time)

                                if can_wait:
                                    reset_time = datetime.datetime.fromtimestamp(reset_time)
                                    sleep_interval = reset_time - datetime.datetime.now() + datetime.timedelta(seconds=30)
                                    sleep_seconds = sleep_interval.total_seconds()
                                    logger.warning('sleeping for %s seconds' % sleep_seconds)
                                    time.sleep(sleep_seconds)
                                    logger.info('waking up')
                                    continue
                                else:
                                    raise TwitterRateLimit(reset_time)
                if can_wait:
                    time.sleep(sleep_time)
                else:
                    raise WebcacheFailure('download failed')
        return None, error

    def download_page(self, url, tries=[1, 2, 4, 8], format='soup', twitter_bearer_token=None, can_wait=True, save_cookies=False):
        '''Downloads page
        and save it in cache
        Silently fails but logs...
        '''
        page, error = self.get_html_from_url(url, tries=tries, format=format, twitter_bearer_token=twitter_bearer_token, can_wait=can_wait, save_cookies=save_cookies)
        if page:
            if format == 'soup':
                self.db.page.save({'url': url, 'date': datetime.datetime.utcnow(), 'page': page})
            elif format == 'json':
                page_to_save = self.replace_in_keys(page, '$', '[dollar]')
                page_to_save = self.replace_in_keys(page_to_save, '.', '[dot]')
                self.db.json.save({'url': url, 'date': datetime.datetime.utcnow(), 'page': page_to_save})
            return page, None
        else:
            return None, error

    def get_page(self, url, interval=datetime.timedelta(hours=20), offline=None, force=None, tries=[1, 2, 4, 8], format='soup', twitter_bearer_token=None, can_wait=True, save_cookies=False):
        '''Gets page if not too old (20 hours by default)...
        interval : maximum age of the page in cache
        force : force download (same as interval = 0)
        offline : do not download the page if not in cache
        Returns the page object or None
        '''
        if offline:
            if format == 'soup':
                page = self.db.page.find_one({'url': url}, sort=[('date', -1)])
            elif format == 'json':
                page = self.db.json.find_one({'url': url}, sort=[('date', -1)])
                if page is not None:
                    page['page'] = page = self.replace_in_keys(page, '[dollar]', '$')
                    page['page'] = page = self.replace_in_keys(page, '[dot]', '.')
            if page is not None:
                page['download_done'] = 0
            return page
        else:
            if format == 'soup':
                page = self.db.page.find_one({'url': url, 'date': {'$gt': datetime.datetime.utcnow() - interval}}, sort=[('date', -1)])
            elif format == 'json':
                page = self.db.json.find_one({'url': url, 'date': {'$gt': datetime.datetime.utcnow() - interval}}, sort=[('date', -1)])
                if page is not None:
                    page['page'] = page = self.replace_in_keys(page, '[dollar]', '$')
                    page['page'] = page = self.replace_in_keys(page, '[dot]', '.')
            if page and not force:
                page['download_done'] = 0
                return page
            else:
                page, error = self.download_page(url, tries=tries, format=format, twitter_bearer_token=twitter_bearer_token, can_wait=can_wait, save_cookies=save_cookies)
                return {'url': url, 'date': datetime.datetime.utcnow(), 'page': page, 'download_done': 1, 'error': error}

if __name__ == '__main__':
    w = w2(host='ks205226.kimsufi.com')
    p = w.get_page('http://graph.facebook.com/remi.douine', format='json')
    pprint(p)
    for i in range(1, 15):
        print i
        try:
            p = w.get_page('https://api.twitter.com/1.1/trends/available.json', format='json', force=True, can_wait=False, twitter_bearer_token='AAAAAAAAAAAAAAAAAAAAAGssRQAAAAAAog54pvHAFants8QdVNUd2A6v%2BVo%3D49jprFA4ZGFVyyUJfkcP5UzZkIWMKSMCeQmNxjjuA')
            pprint(p)
        except TwitterRateLimit as e:
            print e
            reset_time = datetime.datetime.fromtimestamp(e.wait_until)
            sleep_interval = reset_time - datetime.datetime.now() + datetime.timedelta(seconds=30)
            sleep_seconds = sleep_interval.total_seconds()
            print 'sleeping for %s seconds' % sleep_seconds
            time.sleep(sleep_seconds)
            print 'waking up'
            continue
