import redis
import redis_lock
import time

conn = redis.Redis()
#
# lock = redis_lock.Lock(conn, name='my-lock', expire=60, auto_renewal=True)
# if lock.acquire(blocking=False):
#     print 'got the lock!'
# else:
#     print 'Someone got the lock!'
conn.set('fyh', '123')
conn.expire('fyh', 10)