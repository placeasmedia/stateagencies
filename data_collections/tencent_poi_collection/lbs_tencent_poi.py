# -*- coding: utf-8 -*-
'''
Use Tencent Map API to collect POI and save data to a sqlite3 database
'''

import requests
import psycopg2
import time
import datetime
import random
import numpy
import sqlite3
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

API_limit = 1000

API_count = 0

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

# Helper function to load place types
def get_place_type(pfile):
    """
    uafile : string
        path to text file of user agents, one per line
    """
    uas = []
    with open(pfile, 'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uas.append(ua.strip())
    # place = uas.pop()
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

def write_status_to_db(poi):
    # database directory
    write_dir = '/home/users'
    # database file 
    sqlite_file = 'tencentpoi.sqlite'
    # connect to the sqlite database
    conn = sqlite3.connect(write_dir + sqlite_file, timeout=75)
    conn.text_factory = str
    c = conn.cursor()
    # write data into the database
    table_name = 'tencentpoi'
    table_fields = '(id, name, address, tel, category, type, latitude, longitude, distance, postcode, province, city, district, boundary, panoid, panoheading, panopitch, panozoom, search_lat, search_lon, access_time, access_date)'
    values_string = 'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    database_command = 'INSERT INTO ' + table_name + ' ' + values_string 
    database_values = (poi['id'], poi['name'], poi['address'], poi['tel'], poi['category'], poi['type'], poi['latitude'], poi['longitude'], poi['distance'], poi['postcode'], poi['province'], poi['city'], poi['district'], poi['boundary'], poi['panoid'], poi['panoheading'], poi['panopitch'], poi['panozoom'], poi['search_lat'], poi['search_lon'], poi['access_time'], poi['access_date'])
    try:
        c.execute(database_command, database_values)
    except Exception as e:
        print e
        pass
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
        try:
            status_map['id'] = str(status['id'])
            status_map['name'] = str(status['title'])
            try:
                status_map['address'] = str(status['address'])
            except:
                status_map['address'] = 'none'
            try:
                status_map['tel'] = str(status['tel'])
            except:
                status_map['tel'] = 'none'
            try:
                status_map['category'] = str(status['category'])
            except:
                status_map['category'] = 'none'
            try:
                status_map['type'] = str(status['type'])
            except:
                status_map['type'] = 'none'
            try:
                status_map['latitude'] = str(status['location']['lat'])
                status_map['longitude'] = str(status['location']['lng'])
            except:
                status_map['latitude'] = 'none'
                status_map['longitude'] = 'none'
            try:
                status_map['distance'] = str(status['distance'])
            except:
                status_map['distance'] = 'none'
            try:
                status_map['postcode'] = str(status['ad_info']['adcode'])
                status_map['province'] = str(status['ad_info']['province'])
                status_map['city'] = str(status['ad_info']['city'])
                status_map['district'] = str(status['ad_info']['district'])
            except:
                status_map['postcode'] = 'none'
                status_map['province'] = 'none'
                status_map['city'] = 'none'
                status_map['district'] = 'none'

            try:
                status_map['boundary'] = str(status['boundary'])
            except:
                status_map['boundary'] = 'none'
            try:
                status_map['panoid'] = str(status['pano']['id']) 
                status_map['panoheading'] = str(status['pano']['heading']) 
                status_map['panopitch'] = str(status['pano']['pitch']) 
                status_map['panozoom'] = str(status['pano']['zoom']) 
            except:
            	status_map['panoid'] = 'none' 
                status_map['panoheading'] = 'none'
                status_map['panopitch'] = 'none' 
                status_map['panozoom'] = 'none'
        except:
            print status
            raise

        status_map['search_lat'] = search_lat
        status_map['search_lon'] = search_lon
        status_map['access_time'] = current_time
        status_map['access_date'] = current_date

        status_maps.append(status_map)
        
    return (status_maps)


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

    #print 'FINISHED CHECKING PROXY'
    return new_proxy_ip

# Construct baseline URL with input.
def construct_url(lat, lon, place):

    url = 'http://apis.map.qq.com/ws/place/v1/search?'
    # add your API key
    url += 'key=APIKey'
    url += '&boundary=nearby(' + str(lat) + ',' + str(lon) + ',10000)' #radius 10,000 meters 
    url += '&page_size=20'
    url += '&keyword=' + place
    url += '&orderby=_distance'

    
    return url

# Filter request through proxy
def proxy_request(url, uas, proxy_ip):
    global API_count
    proxy = {"http": proxy_ip}
    ua = random.choice(uas)
    headers = {"Connection" : "close", "User-Agent" : ua}
    r = requests.get(url, headers=headers)
    API_count += 1
    return r

def construct_get_url_request(lat, lon, proxy_ip, proxy_count_dict, proxy_time_dict, uas, place):
    remaining = True
    current_statuses = []
    page_num = 1
    total_page_num = 1
    while remaining:

        url = construct_url(lat, lon, place) + '&page_index=' + str(page_num)
        print url 

        proxy_ip = check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict)

        try:
            url_data = proxy_request(url, uas, proxy_ip)
            url_data_json = url_data.json()
            if type(url_data_json) != type(dict()):
                return current_statuses
            proxy_count_dict[proxy_ip] += 1
            error_code = str(url_data_json.get('message'))
            item_count = int(url_data_json.get('count'))
            total_page_num = item_count/20 

            if error_code != 'query ok':
                # reset 
                time.sleep(5)
                time.sleep(random.uniform(40, 60))
                url = construct_url(lat, lon, place) + '&page_index=' + str(page_num)
            else:
                statuses = url_data_json.get('data')
                # print statuses
                for status in statuses:
                    current_statuses.append(status)
                    # print page_num, total_page_num
        except Exception as e:
            print e
            return current_statuses

        if page_num >= total_page_num:
            remaining = False
        else:
            page_num += 1
    return current_statuses

if __name__ == '__main__':
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

    proxy_ip = sorted(proxy_count_dict.keys())[0]

    uas = load_user_agents('user_agents.txt')

    places = get_place_type('tencent_place_types.txt')

    fp = open(location_file, 'rb')
    skip_index = int(sys.argv[3])
    total_counter = 0
    for _ in range(skip_index):
        fp.readline()
    for line in fp:
        for place in places:

            if API_count >= API_limit:
                time.sleep(7200)
                API_count = 0

            elapse_time = time.time() - start_time
            if elapse_time > 86400:
                start_time = time.time()
                API_count = 0

            proxy_ip = check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict)

            line_array = line.strip().split(';')

            lat = str(line_array[1])
            lon = str(line_array[0])

            print('Current latitude: ' + lat + ', current longitude: ' + lon)

            statuses = construct_get_url_request(lat, lon, proxy_ip, proxy_count_dict, proxy_time_dict, uas, place)
            time.sleep(random.uniform(20, 60))
            total_counter += 1
            
            print ('The total counter is now at: ' + str(total_counter)) + ' at ' + str(datetime.datetime.now())
            status_maps = output_statuses_to_maps(statuses, lat, lon)
            for status in status_maps:
                try:
                    write_status_to_db(status)
                except:
                    print status
                    raise
    sys.stdout.flush()

