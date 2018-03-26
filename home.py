#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import random
import time
from datetime import datetime
from elasticsearch import Elasticsearch
import pymongo
import requests
import jsonpath
from multiprocessing.dummy import Pool
import redis


class JoomSpider(object):
    """
        获取joom所有home类目下的商品数据，效果与根据类目id获取数据一样，由于在获取叶子类目和下一页data时是单线程，
        建议分类目爬取，经过验证，这个网站对请求头的反爬只需验证Authorization
    """

    def __init__(self):

        # 首页分类
        self.categories = "https://api.joom.com/1.1/categoriesHierarchy?"

        self.url = "https://api.joom.com/1.1/search/products?language=en&currency=USD"

        self.base_url = "https://www.joom.com/en/products/"

        self.headers_index = {
            'Accept':'*/*',
            'Accept-Encoding':'gzip, deflate, sdch, br',
            'Accept-Language':'zh-CN,zh;q=0.8',
            'Authorization':'Bearer SEV0001MTUxMzkzNTU2OHwzUV9XM3BSVlZERmFBckVIRFNtbGoxRE8yeE5KSUFULV9tVHE4MmdtZENaakNaZllJVzdsM19kMXRzbjFIa04ycV9jRUh5SHBQbjc4MFc4NE5idllTYnJCZ2hiUzVUdUJhbnI4VE1LcFdIWVBpY3F4bXpCZklLRUZRZGVkN2ZkdnkxaWVHWlU5b1hfOHJrYlRPQT09fOqweDu6nkAyq0pGHqNvzKR2J76McOgQZ1kVXRqT8abN',
            'Connection':'keep-alive',
            'Host':'api.joom.com',
            'Origin':'https://www.joom.com',
            'Referer':'https://www.joom.com/en',
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36',
            'X-Version':'0.1.0'
        }

        self.headers_det = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Authorization': 'Bearer SEV0001MTUxMzkzNzc0OXwzTG1IbEhzSWV3MHJFYk5wY3JXUTg5NHRvQ2E5REMxLUExRlNyUEFZcHNNcHJoSVBLZW5OSHZFOTUyemxCRXdXZ3pNdi1aZ1E3OHczU2xoRFU5Y0p5VWl4NVo5bjFsRWgtaEhTeFlOZElTa2t6QlpfeXdLMDFWRTVITC1pbGNhSUdia09POThYNjF4N3RqdWRmZz09fLwstB2ZVllQxA1D6Kb4EpmvOqxcP_NYJo6G4IoOwZDK',
            'Connection': 'keep-alive',
            'Host': 'api.joom.com',
            'Origin': 'https://www.joom.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36',
            'X-Version': '0.1.0'
        }

        self.user_agent_list = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.4",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.109 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/603.2.5 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.5",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
            "Mozilla/5.0 (iPad; CPU OS 10_3_2 like Mac OS X) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.0 Mobile/14F89 Safari/602.1",
            "Mozilla/5.0 (Windows NT 6.1; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.109 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.109 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
            "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1; rv:52.0) Gecko/20100101 Firefox/52.0",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.109 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/58.0.3029.110 Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/603.2.5 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.5",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:53.0) Gecko/20100101 Firefox/53.0",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36 OPR/46.0.2597.32",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/59.0.3071.109 Chrome/59.0.3071.109 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 OPR/45.0.2552.898",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36 OPR/46.0.2597.39",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/602.4.8 (KHTML, like Gecko) Version/10.0.3 Safari/602.4.8",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; Touch; rv:11.0) like Gecko",
            "Mozilla/5.0 (Windows NT 6.1; rv:52.0) Gecko/20100101 Firefox/52.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36",
        ]

        self.mongo_client = pymongo.MongoClient('mongodb://admin1:admin1@..../')

        self.db_name = self.mongo_client["joom_data"]

        self.sheet_name = self.db_name["pro_test"]

        self.sheet_name2 = self.db_name["mer_test"]

        self.sheet_name3 = self.db_name["cate_test"]

        self.client_es = Elasticsearch(hosts=["http://..../",
                                              "http:/..../", "http://..../"])

        self.param = {
            'levels': '1',
            'language': 'en-US',
            'currency': 'USD',
            'categoryId': '1473502936226769818-70-2-118-2760164976',
        }  # 从 home 类读起

        self.categories_name = 'Home'

        self.reviews_param = {
            'filter_id': 'all',
            'count': '8',
            'sort': 'top',
            'language': 'en-US',
            'currency': 'USD'
        }  # 获取评论的请求体

        self.redis_client = redis.Redis(host='....', port=6379, db=1)

    def load_index_categories(self):

        try:

            json_obj = requests.get(self.categories, params=self.param, headers=self.headers_index, timeout=30).json()

        except:

            print("响应超时")

        categories_list = jsonpath.jsonpath(json_obj, "$..payload[children]")[0]

        for categories in categories_list:  # 获取第一个大类home的数据

            categories_id = jsonpath.jsonpath(categories, "$..id")[0]  # 类目id

            hasPublicChildren = jsonpath.jsonpath(categories, "$..hasPublicChildren")[0]

            # print(categories_id, name, hasPublicChildren)

            if hasPublicChildren is True:  # 向下一级一级的读取数据，直到没有下级类目

                self.param["categoryId"] = categories_id
                self.param["parentLevels"] = 1
                self.load_index_categories()

            else:

                data = {"count": 48, "filters": [{"id": "categoryId","value": {"type": "categories","items":
                    [{"id": categories_id}]}}]}   # 根据类目id去获取整个类目下面的商品数据

                # print(data, name)

                joomid_list = set()  # 用集合去重

                self.load_page(data, self.categories_name, joomid_list)

    def load_page(self, data, categories_name, joomid_list):
        """

        :param data:
        :param categories_name:
        :param joomid_list:
        :return: 商品列表页面信息总览,获取商品列表id进入商品详细页面
        """

        try:
            json_obj = requests.post(self.url, data=json.dumps(data), headers=self.headers_index, timeout=30).json()

            result_list = jsonpath.jsonpath(json_obj, "$..payload[item]")[0]

        # post失败，继续post

        except:

            json_obj = requests.post(self.url, data=json.dumps(data), headers=self.headers_index, timeout=30).json()

            try:

                result_list = jsonpath.jsonpath(json_obj, "$..payload[items]")[0]

            except Exception as e:

                print(e)

                print("%s 请求失败" % data)

        # 商品列表页面信息总览

        for result in result_list:

            # 商品ID
            joomid = result.get('id')

            joomid_list.add(joomid)

        try:
            next_page = jsonpath.jsonpath(json_obj, "$..payload")[0]  # 下一页的数据

            # print("%s 发送请求" % next_page["nextPageToken"])

            data["pageToken"] = next_page["nextPageToken"]

            self.load_page(data, categories_name, joomid_list)  # 保存此页面的joomid_list，获取下一页数据

        except:

            print(len(joomid_list))

            print("%s 类有　%d 商品" % (categories_name, len(joomid_list)))

            # 创建线程的线程池
            pool = Pool(15)
            # map()高阶函数，用来批量处理函数传参
            pool.map(self.get_token, joomid_list)
            # 关闭线程池
            pool.close()
            # 阻塞主线程，等待子线程结束
            pool.join()

    def get_token(self, joomid):
        """

        :param joomid:
        :return: 每次传过来的商品获取单独的token
        """

        url = 'https://www.joom.com/tokens/init?'

        UA = random.choice(self.user_agent_list)

        headers = {
            'Accept': '*/*',
            'Acccept-Language': 'zh-CN,zh;q=0.9',
            'Use-Agent': UA,
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://www.joom.com',
            'X-Requested-With': 'XMLHttpRequest',
            'x-version': '0.1.0',
            'content-length': '0',
        }

        token = requests.post(url, headers=headers).json()

        if token:

            accessToken = token.get('accessToken')

            self.redis_client.sadd('token', accessToken)  # 将token保存到redis，以后可以直接用

            #  将id，token和请求头，传到下一个函数

            self.parse_goods(joomid, accessToken, UA)

        else:

            return self.get_token(joomid)

    def parse_goods(self, joomid, accessToken, UA):  # 进入单个商品的链接

        """
        最后获取商品的函数
        :param joomid:
        :param accessToken:
        :param UA:
        :return:
        """
        referer = 'https://www.joom.com/en/products/' + joomid

        headers_det = {

            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Authorization': 'Bearer ' + accessToken,
            'Origin': 'https://www.joom.com',
            'Referer': referer,
            'User-Agent': UA,
            'X-Version': '0.1.0'
        }

        mer_info = {}  # 店铺信息

        cate_info= {}

        print('开始获取{}中的商品：{}\n'.format(self.categories_name, joomid))

        det_url = "https://api.joom.com/1.1/products/" + joomid + "?language=en&currency=USD"

        try:
            json_obj = requests.get(det_url, headers=headers_det, timeout=30).json()

            # 商品总信息
            result = jsonpath.jsonpath(json_obj, "$..payload")[0]

            # 叶子商品类目id

            categoryId = result.get("categoryId")  # 用叶子类目id去获取类目分层关系

            pro_basic = self.get_cate(categoryId)

            cate_info['catename'] = pro_basic['catename']

            cate_info['categoryId'] = pro_basic['categoryId']

            mer_info['catename'] = pro_basic['catename']

            mer_info['categoryId'] = pro_basic['categoryId']

            for i in pro_basic['parents'][-1:]:

                cate_info['top_categoryId'] = i['id']

                cate_info['top_catename'] = i['name']

                mer_info['top_categoryId'] = i['id']

                mer_info['top_catename'] = i['name']

                pro_basic['top_categoryId'] = i['id']

                pro_basic['top_catename'] = i['name']

            # 商品评论总数
            reviews_count = result.get('reviewsCount').get('value')

            if reviews_count >= 100:  # 如果评论数目大于100，就存入redis翻页去获取总评论数

                self.redis_client.sadd('reviews', joomid)  # 将评论大于100的商品保存到redis，以后直接用

                num = 0  # 便于递归函数调用，累加num
                total = 1

                reviews_count = self.get_reviews(joomid, num, accessToken, UA)

                try:

                    while reviews_count < 100 and total < 3:  # 直到读取次数大于３或者评分大于100

                        total += 1

                        print("第{}次获取{}的评论数\n" .format(total, joomid))

                        reviews_count = self.get_reviews(joomid, num, accessToken, UA)

                except Exception as e:

                    print(e)
                    pass

                else:
                    if reviews_count < 100:  # 如果读取3次后，评论还是小于100，就默认为100

                        print('获取:{} 详细评论数失败，默认设置为100'.format(joomid))

                        self.redis_client.sadd('again', joomid)

                        reviews_count = 100

            pro_basic['reviews_count'] = int(reviews_count)

            cate_info['reviews_count'] = int(reviews_count)

            # 商品id
            pro_basic['joomid'] = joomid

            cate_info['joomid'] = joomid

            # 上架时间
            timeStamp = int(joomid[0:10])

            arrival_time = datetime.fromtimestamp(timeStamp).strftime('%Y-%m-%d')  # 转变格式

            pro_basic["arrival_time"] = arrival_time

            # 商品链接
            pro_basic["goods_link"] = 'https://www.joom.com/en/products/' + joomid

            # 店铺ID
            pro_basic['storeId'] = mer_info['storeId'] = cate_info['storeId']= result.get('storeId')

            # 商品描述
            pro_basic["description"] = result.get("description")

            # 商品最高价和最低价
            pro_basic['prices'] = result.get('prices')

            # 商品邮费信息
            pro_basic['shippingPrices'] = result.get('shippingPrices')

            # 商品提问数
            pro_basic['questions_count'] = int(result.get('questionsCount').get('value'))

            # 商品其他图片
            extra_url_list = jsonpath.jsonpath(result, "$..payload[images]")

            pro_basic["extra_url"] = []

            for extra_url in extra_url_list:
                url = jsonpath.jsonpath(extra_url, "$..url")[3]

                pro_basic["extra_url"].append(url)

            # 商品基础信息
            goods_info = jsonpath.jsonpath(result, "$..lite")[0]

            # 商品名称
            pro_basic["name"] = goods_info.get("name")

            # 商品评分
            pro_basic["rating"] = goods_info.get("rating")

            # 商品售卖信息
            pro_basic['sales_count'] = int(goods_info.get('salesCount').get('value'))

            # 商品价格
            pro_basic["price"] = mer_info["price"] = cate_info["price"] = goods_info.get('price')

            # 商品是否打折
            pro_basic['discount'] = goods_info.get('discount')

            # 商品原价
            pro_basic["msrPrice"] = goods_info.get("msrPrice")

            # 商品其他详细信息
            goods_extra_info = jsonpath.jsonpath(result, "$..variants")[0][0]

            # 商品颜色

            pro_basic['colors'] = goods_extra_info.get('colors')

            pro_basic["size"] = goods_extra_info.get("size")

            # 商品库存

            pro_basic["inventory"] = goods_extra_info.get("inventory")

            # 商品运输信息
            pro_basic["shipping"] = goods_extra_info.get('shipping')

            # 运费
            pro_basic["shipping_price"] = goods_extra_info.get("price")

            present = datetime.now().strftime('%Y-%m-%d')

            # 店铺信息

            shop_info = jsonpath.jsonpath(result, "$..store")[0]

            # 店铺更新信息时间
            mer_info['updatedTimeMerchantMs'] = shop_info.get('updatedTimeMerchantMs')

            # 店铺开张时间
            timeS = int(mer_info.get("storeId")[0:10])

            open_time = datetime.fromtimestamp(timeS).strftime('%Y-%m-%d')  # 转变格式

            pro_basic["open_time"] = mer_info["open_time"] = open_time

            # 店铺名称
            mer_info["shop_name"] = pro_basic["shop_name"] = shop_info.get('name')

            # 店铺评分
            mer_info['rating'] = shop_info.get('rating')

            mer_info['positiveReviewsCount'] = shop_info.get('positiveReviewsCount').get('value')

            # 店铺收藏数
            mer_info['favoritesCount'] = shop_info.get('favoritesCount').get('value')

            # 店铺商品总数

            mer_info['productsCount'] = shop_info.get('productsCount').get('value')

            # 店铺评论总数
            mer_info['reviewsCount'] = shop_info.get('reviewsCount').get('value')

            # 店铺是否认证
            mer_info['reviewsCount'] = shop_info.get('reviewsCount').get('value')

            pro_basic['present'] = mer_info['present'] = cate_info['present'] = present

            try:
                self.sheet_name2.insert(dict(mer_info))  # 插入mongo

                self.sheet_name.insert(dict(pro_basic))

                self.sheet_name3.insert_one(dict(cate_info))

            except Exception as e:

                print(e)
                print('商品{}写入Mongodb失败\n'.format(joomid))
                pass

            try:

                pro_resunt = self.client_es.index(index='joom_pro', doc_type='pro_datas', body=dict(pro_basic))  # 商品信息

                # print(pro_resunt)

                mer_result = self.client_es.index(index='joom_mer', doc_type='mer_datas', body=dict(mer_info))  # 店铺信息

                # print(mer_result)

                self.client_es.index(index="joom_cate", doc_type="cate_datas", body=dict(cate_info))

                print('商品{}写入ES成功\n'.format(joomid))

            except Exception as e:

                print(e)

                print('商品{}写入ES失败\n'.format(joomid))

                self.redis_client.sadd('failed', joomid)  # 失败的存入redis中

        except Exception as e:

            print(e)

            print("---------------------------%s\n" % joomid)

            self.redis_client.sadd('failed', joomid)  # 失败的存入redis中

    def get_reviews(self, joomid, num, accessToken, UA):

        """

        :param joomid:
        :param num:
        :param accessToken:
        :param UA:
        :return: 获取评论大于100的商品详细评论数
        """
        # accessToken = self.redis_client.srandmember('token', 1)[0].decode()  # 每次更换token

        reviews_url = "https://api.joom.com/1.1/products/" + joomid + "/reviews?"

        referer = 'https://www.joom.com/en/products/' + joomid

        reviews_headers = {

            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Authorization': 'Bearer ' + accessToken,
            'Referer': referer,
            'Origin': 'https://www.joom.com',
            'User-Agent': UA,
            'X-Version': '0.1.0'
        }

        try:
            json_obj = requests.get(reviews_url, params=self.reviews_param, headers=reviews_headers, timeout=30).json()

        except Exception as e:
            print(e)

        else:

            result = json_obj['payload'].get('nextPageToken')  # 下一页

            item = jsonpath.jsonpath(json_obj, "$..items")[0]

            total = len(item)

            num += total

            if result:  # 判断是否有下一页

                self.reviews_param['pageToken'] = result  # 在请求参数中添加pagetoken

                seconds = [0.1, 0.2, 0.3, 0.1, 0.2]

                time.sleep((random.choice(seconds)))

                return self.get_reviews(joomid, num, accessToken, UA)

            else:

                print('{} 总共有评论数: {}\n'.format(joomid, num))

                return num

    def get_cate(self, categoryId):

        """
        根据叶子类目id获取类目分层关系表
        :param categoryId:
        :param pro_basic:
        :return: 返回给函数parse_goods，继续获取商品信息
        """
        pro_basic = {}

        url = 'https://api.joom.com/1.1/categoriesHierarchy?'

        parmas = {
            'levels': '0',
            'categoryId': categoryId,
            'parentLevels': '-1',
            'language': 'en-US',
            'currency': 'USD',
        }

        try:
            json_obj = requests.get(url, params=parmas, headers=self.headers_det, timeout=30).json()

            parentId = jsonpath.jsonpath(json_obj, "$..parentId")[0]

            # 商品类目名称
            name = jsonpath.jsonpath(json_obj, "$..name")[0]

            # 商品类目id
            categoryId = jsonpath.jsonpath(json_obj, "$..id")[0]

            pro_basic['parentId'] = parentId

            pro_basic['catename'] = name

            pro_basic['categoryId'] = categoryId

        except Exception as e:

            print(e)

        else:

            parents = []

            for i in jsonpath.jsonpath(json_obj, "$..parents")[0]:

                if 'mainImage' in i.keys():
                    i.pop('mainImage')

                if 'hasPublicChildren' in i.keys():
                    i.pop('hasPublicChildren')

                    parents.append(i)

            pro_basic['parents'] = parents

            return pro_basic


if __name__ == "__main__":

    spider = JoomSpider()
    start = time.time()

    spider.load_index_categories()

    print("%s: Useing time %f seconds." % (spider.categories_name, time.time() - start))

