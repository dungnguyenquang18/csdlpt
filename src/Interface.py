#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import time

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='vinh1950', dbname='csdlpt'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    start = time.time()
    con = openconnection
    cur = con.cursor()
    
    # Kiểm tra xem bảng đã tồn tại chưa
    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{ratingstablename}');")
    if cur.fetchone()[0]:
        cur.execute(f"DROP TABLE {ratingstablename};")
    
    # Tạo bảng chính với schema đúng
    cur.execute(f"CREATE TABLE {ratingstablename} (userid INTEGER, movieid INTEGER, rating FLOAT);")
    
    # Tạo bảng tạm để tải dữ liệu
    temp_table = ratingstablename + "_temp"
    cur.execute(f"CREATE TABLE {temp_table} (userid INTEGER, extra1 CHAR, movieid INTEGER, extra2 CHAR, rating FLOAT, extra3 CHAR, timestamp BIGINT);")
    
    # Tải dữ liệu vào bảng tạm
    try:
        with open(ratingsfilepath, 'r') as f:
            cur.copy_from(f, temp_table, sep=':', columns=('userid', 'extra1', 'movieid', 'extra2', 'rating', 'extra3', 'timestamp'))
        
        # Chuyển dữ liệu từ bảng tạm sang bảng chính
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) SELECT userid, movieid, rating FROM {temp_table};")
        
        # Xóa bảng tạm
        cur.execute(f"DROP TABLE {temp_table};")
    except Exception as e:
        print(f"Error loading data: {e}")
        cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
        raise
    
    cur.close()
    con.commit()
    print(f"[loadratings] Thời gian xử lý: {time.time() - start:.4f} giây")

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    start = time.time()
    if numberofpartitions < 1:
        raise ValueError("Number of partitions must be at least 1")
    
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    delta = 5.0 / numberofpartitions
    
    for i in range(numberofpartitions):
        table_name = f"{RANGE_TABLE_PREFIX}{i}"
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
        min_range = i * delta
        max_range = (i + 1) * delta
        # Sử dụng ">=" cho phân mảnh đầu tiên để bao gồm rating = 0
        if i == 0:
            cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) "
                        f"SELECT userid, movieid, rating FROM {ratingstablename} "
                        f"WHERE rating >= {min_range} AND rating <= {max_range};")
        else:
            cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) "
                        f"SELECT userid, movieid, rating FROM {ratingstablename} "
                        f"WHERE rating > {min_range} AND rating <= {max_range};")
    
    cur.close()
    con.commit()
    print(f"[rangepartition] Thời gian xử lý: {time.time() - start:.4f} giây")

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    start = time.time()
    if numberofpartitions < 1:
        raise ValueError("Number of partitions must be at least 1")
    
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) "
                    f"SELECT userid, movieid, rating FROM "
                    f"(SELECT userid, movieid, rating, ROW_NUMBER() OVER () - 1 AS rnum FROM {ratingstablename}) t "
                    f"WHERE rnum % {numberofpartitions} = {i};")
    
    cur.close()
    con.commit()
    print(f"[roundrobinpartition] Thời gian xử lý: {time.time() - start:.4f} giây")

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    start = time.time()
    if not (0 <= rating <= 5):
        raise ValueError("Rating must be between 0 and 5")
    
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Chèn vào bảng chính
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) "
                f"VALUES ({userid}, {itemid}, {rating});")
    
    # Lấy số thứ tự bản ghi (dùng meta-data hoặc biến tạm)
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows - 1) % numberofpartitions
    table_name = f"{RROBIN_TABLE_PREFIX}{index}"
    
    # Chèn vào phân mảnh
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) "
                f"VALUES ({userid}, {itemid}, {rating});")
    
    cur.close()
    con.commit()
    print(f"[roundrobininsert] Thời gian xử lý: {time.time() - start:.6f} giây")

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    start = time.time()
    if not (0 <= rating <= 5):
        raise ValueError("Rating must be between 0 and 5")
    
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5.0 / numberofpartitions
    
    # Xác định phân mảnh
    index = None
    for i in range(numberofpartitions):
        min_range = i * delta
        max_range = (i + 1) * delta
        if i == 0 and rating >= min_range and rating <= max_range:
            index = i
            break
        elif rating > min_range and rating <= max_range:
            index = i
            break
    if index is None:
        index = numberofpartitions - 1  # Trường hợp rating = 5
    
    table_name = f"{RANGE_TABLE_PREFIX}{index}"
    
    # Chèn vào bảng chính
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) "
                f"VALUES ({userid}, {itemid}, {rating});")
    
    # Chèn vào phân mảnh
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) "
                f"VALUES ({userid}, {itemid}, {rating});")
    
    cur.close()
    con.commit()
    print(f"[rangeinsert] Thời gian xử lý: {time.time() - start:.6f} giây")

def create_db(dbname):
    try:
        con = getopenconnection(dbname='postgres')
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = '{dbname}';")
        if cur.fetchone()[0] == 0:
            cur.execute(f"CREATE DATABASE {dbname};")
        cur.close()
        con.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        raise

def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '{prefix}%';")
    count = cur.fetchone()[0]
    cur.close()
    return count