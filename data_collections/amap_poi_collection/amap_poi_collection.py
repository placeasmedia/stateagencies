# -*- coding: utf-8 -*-
'''
Use Gaode Map API to collect POI and save data into PostgreSQL
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

# API limit daily 1000 and 50/second
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

# retrieve records 
def _retrieve_poi():
    '''
    retrieve record from database for checking duplicates
    '''
    conn_string = "host='localhost' dbname='database' user='postgres' password='password'"
    # print the connection string we will use to connect
    #print "Retrieve records: connecting to database\n    ->%s" % (conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()

    select_sql = "select latitude, longitude, gid from amapdata where lng_wgs is null limit 100"

    #print select_sql

    cursor.execute(select_sql)

    # retrieve the records from the database
    records = cursor.fetchall()

    return records

# write records to postgreSQL
def write_status_to_db(poi):
    # database configuration
    conn_string = "host='localhost' dbname='database' user='postgres' password='password'"

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    c = conn.cursor()

    table_name = 'amappoi'
    table_fields = '(id, name, type, typecode, biz_type, address, latitude, longitude, telphone, distance, biz_ext, province, city, district, importance, shopid, shopinfo, poiweight, search_lat, search_lon, access_time, access_date)'
    values_string = 'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    database_command = 'INSERT INTO ' + table_name + ' ' + values_string 
    database_values = (poi['id'], poi['name'], poi['type'], poi['typecode'], poi['biz_type'], poi['address'], poi['latitude'], poi['longitude'], poi['telphone'], poi['distance'], poi['biz_ext'], poi['province'], poi['city'], poi['district'], poi['importance'], poi['shopid'], poi['shopinfo'], poi['poiweight'], poi['search_lat'], poi['search_lon'], poi['access_time'], poi['access_date'])
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
    for status in statuses:
        status_map = dict()
        # add fields into map
        try:
            status_map['id'] = str(status['id'])
            status_map['name'] = str(status['name'])
            try:
                status_map['type'] = str(status['type'])
            except:
                status_map['type'] = 'none'
            try:
                status_map['typecode'] = str(status['typecode'])
            except:
                status_map['typecode'] = 'none'
            try:
                status_map['biz_type'] = str(status['biz_type'])
            except:
                status_map['biz_type'] = 'none'
            try:
                status_map['address'] = str(status['address'])
            except:
                status_map['address'] = 'none'
            try:
                status_map['latitude'] = str(status['location'].split(',')[1])
                status_map['longitude'] = str(status['location'].split(',')[0])
            except:
                status_map['latitude'] = 'none'
                status_map['longitude'] = 'none'
            try:
                status_map['telphone'] = str(status['tel'])
            except:
                status_map['telphone'] = 'none'
            try:
                status_map['distance'] = str(status['distance'])
            except:
                status_map['distance'] = 'none'
            try:
                status_map['biz_ext'] = str(status['biz_ext'])
            except:
                status_map['biz_ext'] = 'none'
            try:
                status_map['province'] = str(status['pname'])
            except:
                status_map['province'] = 'none'
            try:
                status_map['city'] = str(status['cityname'])
            except:
                status_map['city'] = 'none'
            try:
                status_map['district'] = str(status['adname'])
            except:
                status_map['district'] = 'none'
            try:
                status_map['importance'] = str(status['importance'])
            except:
                status_map['importance'] = 'none'
            try:
                status_map['shopid'] = str(status['shopid'])
            except:
                status_map['shopid'] = 'none'
            try:
                status_map['shopinfo'] = str(status['shopinfo'])
            except:
                status_map['shopinfo'] = 'none'
            try:
                status_map['poiweight'] = str(status['poiweight'])
            except:
                status_map['poiweight'] = 'none'
        except:
            print status
            raise

        status_map['search_lat'] = search_lat
        status_map['search_lon'] = search_lon
        status_map['access_time'] = current_time
        status_map['access_date'] = current_date

        status_maps.append(status_map)
        
    return (status_maps)

# Use proxy in case IP outside of China is banned
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

# Construct =URL with Amap API
def construct_url(lat, lon, place):
    # Amap API
    url = 'http://restapi.amap.com/v3/place/around?'
    # Add API key
    url += 'key=APIKEY' 
    url += '&location=' + str(lon) + ',' + str(lat)
    url += '&radius=1000'
    # type of place in Amap -- see the other file "amap_place_types"
    url += '&types=' + str(place)
    return url

# Filter request through proxy
def proxy_request(url, uas, proxy_ip):
    global API_count
    proxy = {"http": proxy_ip}
    # print proxy
    ua = random.choice(uas)
    # print ua
    headers = {"Connection" : "close", "User-Agent" : ua}
    # proxy needs fix; requests bug
    # r = requests.get(url, proxies=proxy, headers=headers)
    r = requests.get(url, headers=headers)

    # Add one count for API
    API_count += 1

    return r

# construct an url for requests
def construct_get_url_request(lat, lon, proxy_ip, proxy_count_dict, proxy_time_dict, uas, place):
    remaining = True
    current_statuses = []
    page_num = 0
    total_page_num = 1
    while remaining:

        url = construct_url(lat, lon, place) + '&page=' + str(page_num)
        print url 

        # Check proxy
        # proxy_ip = check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict)

        try:
            url_data = proxy_request(url, uas, proxy_ip)
            # print url_data
            url_data_json = url_data.json()
            if type(url_data_json) != type(dict()):
                return current_statuses
            proxy_count_dict[proxy_ip] += 1
            error_code = str(url_data_json.get('status'))
            item_count = int(url_data_json.get('count'))
            total_page_num = item_count/20 

            if error_code != '1':
                # reset 
                time.sleep(random.uniform(4, 10))
                url = construct_url(lat, lon, place) + '&page=' + str(page_num)
            else:
                statuses = url_data_json.get('pois')
                for status in statuses:
                    current_statuses.append(status)
        except Exception as e:
            print e
            return current_statuses

        # Update proxy counts
        #proxy_count_dict[proxy_ip] += 1

        if page_num >= total_page_num:
            remaining = False
        else:
            page_num += 1

    return current_statuses

if __name__ == '__main__':
    # IP proxy file
    proxy_file = sys.argv[1]
    # file of search points
    location_file = sys.argv[2]
    #load proxies
    fin = open(proxy_file, 'rb')
    proxy_count_dict = dict()
    proxy_time_dict = dict()
    for line in fin:
        proxy_ip = line.strip()
        proxy_count_dict[proxy_ip] = 0
        proxy_time_dict[proxy_ip] = datetime.datetime.min

    # select proxy randomly
    proxy_ip = sorted(proxy_count_dict.keys())[0]
    # print proxy_ip

    # load agents
    uas = load_user_agents('user_agents.txt')

    # load place types in Amap
    places = get_place_type('amap_place_types.txt')

    fp = open(location_file, 'rb')
    skip_index = int(sys.argv[3])
    total_counter = 0
    for _ in range(skip_index):
        fp.readline()
    for line in fp:
        total_counter += 1
        for place in places:
            # Check if API count reach the daily limit
            if API_count >= API_limit:
                time.sleep(36000)
                API_count = 0

            # proxy_ip = check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict)
            
            line = line.replace('"', '')
            line_array = line.strip().split(',')

            lat = str(line_array[1])
            lon = str(line_array[0])

            print('Current latitude: ' + lat + ', current longitude: ' + lon)

            statuses = construct_get_url_request(lat, lon, proxy_ip, proxy_count_dict, proxy_time_dict, uas, place)
            # print statuses
            time.sleep(random.uniform(0, 2))
            
            print ('The total counter is now at: ' + str(total_counter)) + ' at ' + str(datetime.datetime.now())
            status_maps = output_statuses_to_maps(statuses, lat, lon)
            for status in status_maps:
                try:
                    write_status_to_db(status)
                except:
                    print status
                    raise
    sys.stdout.flush()
