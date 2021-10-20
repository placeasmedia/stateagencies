# -*- coding: utf-8 -*-
'''
Use Google Map API to collect POI
'''

import requests
import psycopg2
import time
import datetime
import random
import numpy
import sqlite3
import sys


# API limit daily 1000 and 50/second
API_limit = 1000

API_count = 0

reload(sys)
sys.setdefaultencoding('utf-8')

USER_AGENTS_FILE = 'user_agents.txt'

# Helper function to load user agents.
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

# write records to a sqlite database
def write_status_to_db(poi):
    # writing directory for the database file
    write_dir = '/scratch/users/datalama/' 
    # use a sqlite database to save the POI data
    sqlite_file = 'googlepoi.sqlite'
    conn = sqlite3.connect(write_dir + sqlite_file, timeout=75)
    conn.text_factory = str
    c = conn.cursor()
    table_name = 'googlepoi'
    table_fields = '(name, latitude, longitude, place_id, vicinity, reference, photos_reference, html_attributions, id, types, icon, search_lat, search_lon, access_time, access_date)'
    values_string = 'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    database_command = 'INSERT INTO ' + table_name + ' ' + values_string 
    database_values = (poi['name'], poi['latitude'], poi['longitude'], poi['place_id'], poi['vicinity'], poi['reference'], poi['photos_reference'], poi['html_attributions'], poi['id'], poi['types'], poi['icon'], poi['search_lat'], poi['search_lon'], poi['access_time'], poi['access_date'])
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
            status_map['latitude'] = str(status['geometry']['location']['lat'])
            status_map['longitude'] = str(status['geometry']['location']['lng'])
            try:
                status_map['place_id'] = str(status['place_id'])
            except:
                status_map['place_id'] = 'none'
            try:
                status_map['vicinity'] = str(status['vicinity'])
            except:
                status_map['vicinity'] = 'none'
            try:
                status_map['reference'] = str(status['reference'])
            except:
                status_map['reference'] = 'none'
            try:
                status_map['photos_reference'] = str(status['photos']['photo_reference'])
            except:
                status_map['photos_reference']  = 'none'
            try:
                status_map['html_attributions'] = str(status['photos']['html_attributions'])
            except:
                status_map['html_attributions']  = 'none'
            try:
                status_map['id'] = str(status['id'])
            except:
                status_map['id'] = status_map['latitude'] + ',' + status_map['longitude'] 
            try:
                status_map['types'] = str(status['types'])
            except:
                status_map['types'] = 'none'
            try:
                status_map['icon'] = str(status['icon'])
            except:
                status_map['icon'] = 'none'
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

    return new_proxy_ip

# Construct baseline URL with input.
def construct_url(lat, lon, place):
    url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
    # Add your API key
    url += 'key=APIKey' 
    url += '&location=' + str(lat) + ',' + str(lon)
    url += '&radius=10000'
    url += '&type=' + place
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
    API_count += 1

    return r

def construct_get_url_request(lat, lon, proxy_ip, proxy_count_dict, proxy_time_dict, uas, place):
    remaining = True
    current_statuses = []
    page_num = 0
    total_page_num = 1
    url = construct_url(lat, lon, place)
    
    # Check proxy.
    proxy_ip = check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict)
    # print proxy_ip

    # Request using proxy.
    try:
        url_data = proxy_request(url, uas, proxy_ip)
    except Exception as e:
        print e
        return current_statuses
    
    # print url_data
    url_data_json = url_data.json()
    if type(url_data_json) != type(dict()):
        return current_statuses
    proxy_count_dict[proxy_ip] += 1
    error_code = str(url_data_json.get('status'))

    if error_code == 'ZERO_RESULTS':
        print url
        return current_statuses
    elif error_code == 'INVALID_REQUEST':
        return current_statuses
    else:
        statuses = url_data_json.get('results')
        for status in statuses:
            current_statuses.append(status)
        # next page
        try:
            next_page = url_data_json.get('next_page_token')
            if str(next_page) == 'None':
                return current_statuses
                remaining = False
        except:
            return current_statuses
        url_next = url + '&pagetoken=' + str(next_page)
        while remaining:
            print url_next
            time.sleep(random.uniform(2, 10))
            url_data = proxy_request(url_next, uas, proxy_ip)
            url_data_json = url_data.json()
            if type(url_data_json) != type(dict()):
                return current_statuses
                remaining = False
            proxy_count_dict[proxy_ip] += 1
            error_code = str(url_data_json.get('status'))
            if error_code == 'ZERO_RESULTS':
                print url
                return current_statuses
            elif error_code == 'INVALID_REQUEST':
                return current_statuses
            else:
                statuses = url_data_json.get('results')
                for status in statuses:
                    current_statuses.append(status) 
                try:
                    next_page = url_data_json.get('next_page_token')
                    if str(next_page) == 'None':
                        return current_statuses
                        remaining = False
                    else:
                        url_next = url + '&pagetoken=' + next_page    
                except:
                    remaining = False

    return current_statuses

if __name__ == '__main__':
    proxy_file = sys.argv[1]
    location_file = sys.argv[2]
    #load proxies
    fin = open(proxy_file, 'rb')
    proxy_count_dict = dict()
    proxy_time_dict = dict()
    for line in fin:
        proxy_ip = line.strip()
        proxy_count_dict[proxy_ip] = 0
        proxy_time_dict[proxy_ip] = datetime.datetime.min

    proxy_ip = sorted(proxy_count_dict.keys())[0]

    uas = load_user_agents('user_agents.txt')

    places = get_place_type('google_place_types.txt')

    fp = open(location_file, 'rb')
    skip_index = int(sys.argv[3])
    total_counter = 0
    for _ in range(skip_index):
        fp.readline()
    for line in fp:
        for place in places:
            proxy_ip = check_get_proxy(proxy_ip, proxy_count_dict, proxy_time_dict)

            time.sleep(random.uniform(0, 2))

            line_array = line.strip().split(',')

            lat = str(line_array[2])
            lon = str(line_array[1])

            print('current place type: ' + place)

            statuses = construct_get_url_request(lat, lon, proxy_ip, proxy_count_dict, proxy_time_dict, uas, place)
            total_counter += 1
            
            print ('The total counter is now at: ' + str(total_counter)) + ' at ' + str(datetime.datetime.now())
            status_maps = output_statuses_to_maps(statuses, lat, lon)
            for status in status_maps:
                try:
                    # print status
                    write_status_to_db(status)
                except:
                    print status
                    raise
    sys.stdout.flush()
