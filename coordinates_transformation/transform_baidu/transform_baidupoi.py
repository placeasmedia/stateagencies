# -*- coding: utf-8 -*-
'''
Correct offset by the coordinate system of Baidu (bd09)

'''
import psycopg2
import coordinates_transformation as ct


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

    select_sql = "select latitude, longitude, uid from baidupoi where lng_wgs is null limit 1000"

    #print select_sql

    cursor.execute(select_sql)

    # retrieve the records from the database
    records = cursor.fetchall()

    return records

def _update_poi(id, lat, lng):
    conn_string = "host='localhost' dbname='database' user='postgres' password='password'"
    # print the connection string we will use to connect
    #print "Retrieve records: connecting to database\n    ->%s" % (conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()

    update_sql = "update baidupoi set lat_wgs = %s, lng_wgs = %s where uid =  \'%s\'" % (lat, lng, id)
    #print select_sql

    cursor.execute(update_sql)

    conn.commit()


if __name__ == '__main__':
    for n in range(1000):
        records = _retrieve_poi()
        for record in records:
            latitude = record[0]
            longitude = record[1]
            uid = record[2]

            latitude = float(latitude)
            longitude = float(longitude)
            (lng_gcj, lat_gcj) = ct.bd09togcj02(longitude, latitude)
            (lng_wgs, lat_wgs) = ct.gcj02towgs84(lng_gcj, lat_gcj)
            # print lng_wgs, lat_wgs
            _update_poi(uid, lat_wgs, lng_wgs)

    print("it's done!")