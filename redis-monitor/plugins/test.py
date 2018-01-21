import redis
import ujson

conn = redis.StrictRedis()

# conn.zadd('fyh', 2, 'lllll')
for item in conn.zscan_iter('fyh'):
     # print ujson.loads(item[0])
    print (item)