# -*- coding: utf-8 -*-
'''
Use Baidu Map API to collect POIs and save data into PostgreSQL

Baidu categorization
一级行业分类  二级行业分类
美食  中餐厅、外国餐厅、小吃快餐店、蛋糕甜品店、咖啡厅、茶座、酒吧
酒店  星级酒店、快捷酒店、公寓式酒店
购物  购物中心、超市、便利店、家居建材、家电数码、商铺、集市
生活服务    通讯营业厅、邮局、物流公司、售票处、洗衣店、图文快印店、照相馆、房产中介机构、公用事业、维修点、家政服务、殡葬服务、彩票销售点、宠物服务、报刊亭、公共厕所
丽人  美容、美发、美甲、美体
旅游景点    公园、动物园、植物园、游乐园、博物馆、水族馆、海滨浴场、文物古迹、教堂、风景区
休闲娱乐    度假村、农家院、电影院、KTV、剧院、歌舞厅、网吧、游戏场所、洗浴按摩、休闲广场
运动健身    体育场馆、极限运动场所、健身中心
教育培训    高等院校、中学、小学、幼儿园、成人教育、亲子教育、特殊教育学校、留学中介机构、科研机构、培训机构、图书馆、科技馆
文化传媒    新闻出版、广播电视、艺术团体、美术馆、展览馆、文化宫
医疗  综合医院、专科医院、诊所、药店、体检机构、疗养院、急救中心、疾控中心
汽车服务    汽车销售、汽车维修、汽车美容、汽车配件、汽车租赁、汽车检测场
交通设施    飞机场、火车站、地铁站、长途汽车站、公交车站、港口、停车场、加油加气站、服务区、收费站、桥
金融  银行、ATM、信用社、投资理财、典当行
房地产     写字楼、住宅区、宿舍
公司企业    公司、园区、农林园艺、厂矿
政府机构    中央机构、各级政府、行政单位、公检法机构、涉外机构、党派团体、福利机构、政治教育机构 

'''

import requests
import psycopg2
import time
import datetime
import random
import numpy
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

# API Key
ak = 'APIkey'

USER_AGENTS_FILE = 'user_agents.txt'

# Helper function to oad user agents.
def load_user_agents(uafile=USER_AGENTS_FILE):
    """
    uafile : string
        path to text file of user agents, one per line
    """
    uas = []
    with open(uafile, 'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uas.append(ua.strip()[1:-1-1])
    random.shuffle(uas)
    return uas


# decode list to utf8
def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv

# decode dictionary to utf8
def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv

# write records to database
def write_status_to_db(poi):
    # configure postgreSQL
    conn_string = "host='localhost' dbname='database' user='postgres' password='password'"
    # print the connection string we will use to connect
    #print "Retrieve records: connecting to database\n    ->%s" % (conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    c = conn.cursor()
    table_name = 'baidupoi'
    table_fields = '(name, latitude, longitude, address, street_id, telephone, detail, uid, tag, navi_lat, navi_lng, type, detail_url, overall_rating, comment_num, search_lat, search_lon, access_time, access_date)'
    values_string = 'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    database_command = 'INSERT INTO ' + table_name + ' ' + values_string 
    database_values = (poi['name'], poi['latitude'], poi['longitude'], poi['address'], poi['street_id'], poi['telephone'], poi['detail'], poi['uid'], poi['tag'], poi['navi_lat'], poi['navi_lng'], poi['type'], poi['detail_url'], poi['overall_rating'], poi['comment_num'], poi['search_lat'], poi['search_lon'], poi['access_time'], poi['access_date'])
    try:
        c.execute(database_command, database_values)
    except Exception as e:
        print e

    conn.commit()
    conn.close()

# Convert larger list of maps into smaller list of relevant maps.
def output_statuses_to_maps(statuses, search_lat, search_lon):
    status_maps = []
    current_time = str(datetime.datetime.now())
    current_date = current_time.split(' ')[0]
    # statuses = _decode_dict(statuses)
    for status in statuses:
        status_map = dict()
        # add fields into map
        status_map['name'] = str(status['name'])
        try:
            try:
                localtion = str(status['location'])
                status_map['latitude'] = str(status['location']['lat'])
                status_map['longitude'] = str(status['location']['lng'])
            except:
                print status
                # no location
                return (status_maps)
            try:
                status_map['address'] = str(status['address'])
            except:
                status_map['address'] = 'none'
            try:
                status_map['street_id'] = str(status['street_id'])
            except:
                status_map['street_id'] = 'none'
            try:
                status_map['telephone'] = str(status['telephone'])
            except:
                status_map['telephone'] = 'none'
            try:
                status_map['detail'] = str(status['detail'])
            except:
                status_map['detail'] = 'none'
            try:
                status_map['uid'] = str(status['uid'])
            except:
                status_map['uid'] = status_map['latitude'] + ',' + status_map['longitude'] 
            try:
                status_map['tag'] = str(status['detail_info']['tag'])
            except:
                status_map['tag'] = 'none'
            try:
                status_map['navi_lat'] = str(status['detail_info']['navi_location']['lat'])
                status_map['navi_lng'] = str(status['detail_info']['navi_location']['lng'])
            except:
                status_map['navi_lat'] = 'none'
                status_map['navi_lng'] = 'none'
            try:
                status_map['type'] = str(status['detail_info']['type'])
                status_map['detail_url'] = str(status['detail_info']['detail_url'])
            except:
                status_map['type'] = 'none'
                status_map['detail_url'] = 'none'
            try:
                status_map['overall_rating'] = str(status['detail_info']['overall_rating'])
                status_map['comment_num'] = str(status['detail_info']['comment_num'])
            except:
                status_map['overall_rating'] = 'none'
                status_map['comment_num'] = 'none'

        except:
            print status
            raise

        status_map['search_lat'] = search_lat
        status_map['search_lon'] = search_lon
        status_map['access_time'] = current_time
        status_map['access_date'] = current_date

        status_maps.append(status_map)
        
    return (status_maps)

# Check proxy
def check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict):
    if (proxy_count_dict[proxy_ip] < 1000) and (abs(proxy_time_dict[proxy_ip] - datetime.datetime.now()) > datetime.timedelta(hours=1)):
        return proxy_ip

    proxy_list = proxy_count_dict.keys()
    found = False
    for proxy in proxy_list:
        proxy_count = proxy_count_dict[proxy]
        proxy_time = proxy_time_dict[proxy]
        current_time = datetime.datetime.now()
        proxy_time_diff = abs(current_time - proxy_time)
        if (proxy_count == 0) and (proxy_time_diff > datetime.timedelta(hours=1)):
            found = True
            new_proxy_ip = proxy
            break

    proxy_count_dict[proxy_ip] = 0
    proxy_time_dict[proxy_ip] = datetime.datetime.now()

    return new_proxy_ip

# Construct Baidu API URL with parameters
def construct_url(lat, lon):

    lower_latitude = float(lat) - 0.05
    upper_latitude = float(lat) + 0.05
    lower_longitude = float(lon) - 0.05
    upper_longitude = float(lon) + 0.05
    bound = str(lower_latitude) + ',' + str(lower_longitude) + ',' + str(upper_latitude) + ',' + str(upper_longitude)

    # query a specific place type
    url = 'http://api.map.baidu.com/place/v2/search?query=清真寺&page_size=20&scope=2&output=json&ak=APIkey'
    boundary = '&bounds=%s'%(bound)

    url = url + boundary
    
    return url

# Filter request through proxy
def proxy_request(url, uas, proxy_ip):
    global API_count
    proxy = {"http": proxy_ip}
    # print proxy
    ua = random.choice(uas)
    # print ua
    headers = {"Connection" : "close", "User-Agent" : ua}

    r = requests.get(url, headers=headers)

    # increment API count
    API_count += 1

    return r

def construct_get_url_request(lat, lon, proxy_ip, proxy_count_dict, proxy_time_dict, uas):
    # Wrap in for loop.
    remaining = True
    current_statuses = []
    page_num = 0
    total_page_num = 1
    while remaining:

        url = construct_url(lat, lon) + '&page=' + str(page_num)
        print url 

        # Check proxy.
        # proxy_ip = check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict)
        # print proxy_ip

        # Request using proxy.
        found = False
        while not found:
            try:
                url_data = proxy_request(url, uas, proxy_ip)
                # print url_data
                url_data_json = url_data.json()
                if type(url_data_json) != type(dict()):
                    return current_statuses
                proxy_count_dict[proxy_ip] += 1
                error_code = str(url_data_json.get('message'))
                #print 'Error code: ' + error_code

                if error_code != 'ok':
                    # reset 
                    time.sleep(random.uniform(40, 60))
                    url = construct_url(lat, lon) + '&page=' + str(page_num)
                else:
                    found = True
                    statuses = url_data_json.get('results')
                    # print statuses
                    total_num = int(url_data_json.get('total'))

                    for status in statuses:
                        current_statuses.append(status)
                    total_page_num = total_num / 20
            except Exception as e:
                print e
                return current_statuses

        # Update proxy counts.
        #proxy_count_dict[proxy_ip] += 1

        # Check termination.
        if page_num > total_page_num:
            remaining = False
        else:
            page_num += 1

    return current_statuses

if __name__ == '__main__':
    global API_count
    global API_limit
    # daily API limit
    API_limit = 1000
    API_count = 0
    
    proxy_file = sys.argv[1]
    location_file = sys.argv[2]
    #load proxies
    fin = open(proxy_file, 'rb')
    proxy_count_dict = dict()
    proxy_time_dict = dict()

    start_time = time.time()

    for line in fin:
        proxy_ip = line.strip()
        proxy_count_dict[proxy_ip] = 0
        proxy_time_dict[proxy_ip] = datetime.datetime.min

    # select proxy randomly
    proxy_ip = sorted(proxy_count_dict.keys())[0]
    # print proxy_ip

    uas = load_user_agents('user_agents.txt')

    fp = open(location_file, 'rb')
    skip_index = int(sys.argv[3])
    total_counter = 0
    for _ in range(skip_index):
        fp.readline()
    for line in fp:
        if API_count > API_limit:
            time.sleep(36000)
            API_count = 0

        elapse_time = time.time() - start_time
        if elapse_time > 86400:
            start_time = time.time()
            API_count = 0
            
        # proxy_ip = check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict)

        line = line.replace('"', '')
        line_array = line.strip().split(',')

        time.sleep(random.uniform(1, 2))

        lat = str(line_array[1])
        lon = str(line_array[0])

        print('Current latitude: ' + lat + ', current longitude: ' + lon)

        statuses = construct_get_url_request(lat, lon, proxy_ip, proxy_count_dict, proxy_time_dict, uas)
        total_counter += 1
        
        #print ('Found this many statuses: ' + str(len(statuses)))
        print ('The total counter is now at: ' + str(total_counter)) + ' at ' + str(datetime.datetime.now())
        status_maps = output_statuses_to_maps(statuses, lat, lon)
        for status in status_maps:
            try:
                write_status_to_db(status)
            except:
                print status
                raise
    sys.stdout.flush()










