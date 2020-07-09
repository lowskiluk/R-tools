import sys
import redis

def check_big_key(r, k):
    bigKey = False
    length = 0
    try:
        type = r.type(k)
        if type == "string":
            length = r.strlen(k)
        elif type == "hash":
            length = r.hlen(k)
        elif type == "list":
            length = r.llen(k)
        elif type == "set":
            length = r.scard(k)
        elif type == "zset":
            length = r.zcard(k)
    except:
        return
    if length > 10240:
        bigKey = True
    if bigKey:
        print db,k,type,length

def find_big_key_normal(db_host, db_port, db_password, db_num):
    r = redis.StrictRedis(host=db_host, port=db_port, password=db_password, db=db_num)
    for k in r.scan_iter(count=1000):
        check_big_key(r, k)


# your db info
db_host = "000.000.000.000"
db_port = "8888"
db_password = "xxxxxxxx"
db=0

find_big_key_normal(db_host, db_port, db_password, db)