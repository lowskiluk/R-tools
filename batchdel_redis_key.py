# -*- coding: utf-8 -*-
import sys
import redis
from itertools import izip_longest
import time

# iterate a list in batches of size n
def batcher(iterable, n):
    args = [iter(iterable)] * n
    return izip_longest(*args)

def getRedisConn(db_host, db_port, db_password, db_num):
    try:
        # your redis server info.
        #response = redis.StrictRedis(host='000.000.000.000', port=9999, db=1, password='')
        response = redis.StrictRedis(host=db_host, port=db_port, password=db_password, db=db_num)
        if response.ping():
            print("redis conn success!")
    except redis.ConnectionError , e:
        #your error handlig code here 
        print("redis conn error.")
        print(str(e))
        sys.exit(0)

    return response

def count_key(key_name,r):
    del_count = 0
    print(r.ping())

    print("Begin calc the total number of key prefix:"+key_name)

    # in batches of 20000
    for key in r.scan_iter(key_name,20000):
        del_count += 1
        if(del_count > 1000000):
            print("Total number of key_prefix:"+key_name+" is more than 2000000,stop calc the number and run del_big_set_key function directly.")
            break

    if(del_count <= 1000000):
        print("Total number of key_prefix:"+key_name+" is:"+bytes(del_count))

    return del_count

def del_small_set_key(key_name,r):
    del_count = 0
    print(r.ping())

    print("Batch del key prefix:"+key_name)
    print("Begin del redis key...")

    # in batches of 20000 delete keys matching user:*
    for key in r.scan_iter(key_name,20000):
        r.delete(key)
        del_count += 1
        #print(key)
        #print "total del keys:",del_count,"time:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());
    return del_count

def del_big_set_key(key_name,r):
    del_count = 0
    print(r.ping())

    print("Batch del key prefix:"+key_name)
    print("Begin del redis key...")

    # in batches of 5000 delete keys matching user:*
    for keybatch in batcher(r.scan_iter(key_name,20000),20000):
        r.delete(*keybatch)
        del_count += len(keybatch)
        #print(keybatch)
        #print "total del keys:",del_count,"time:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());
    return del_count



print("Process begin.====================================================")
begintime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

################# Enter your db and what key prefix you want to find&delete. #################

# 1.your db info
db_host = "000.000.000.000"
db_port = "8888"
db_password = "xxxxxxxx"
db_num = 0

# 2.define the key prefix what you want to batch delete. '*' in the end is necessary.
key_prefix = 'your_key_prefix*'

################# end ########################################################################


#Get redis connection.
r = getRedisConn(db_host, db_port, db_password, db_num)

#Get total number of maching keys.
tnum = count_key(key_prefix,r)

if(tnum > 1000000):
    print("Total keys more than 1000000,run del_big_set_key function:")
    dnum = del_big_set_key(key_prefix,r)
else:
    print("Total keys less than 1000000,run del_small_set_key function:")
    dnum = del_small_set_key(key_prefix,r)


endtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

#print info.
print("Process end.======================================================")
print("Begin time:"+begintime)
print("End time:"+endtime)
print "Total delete count:",dnum;