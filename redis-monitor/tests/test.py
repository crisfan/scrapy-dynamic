# coding:utf-8
import redis
import redis_lock
import time

conn = redis.Redis()  # 获取redis连接


# 创建一个锁实例
# lock = redis_lock.Lock(conn, "name-of-the-lock")
#
# if lock.acquire(blocking=False):
#     print("Got the lock.")
# else:
#     print("Someone else has the lock.")

'''
在某些情况下，锁始终保持在redis中，如果服务器停电、redis或应用程序崩溃/未处理的异常，在这种情况下，通过重新启动应用程序并不会删除锁定，
一种解决方法是打开auto_renewal参数并结合expire来设置锁定超时，让Lock()会自动保持应用程序代码执行期间的过期时间。
'''

# one
# Get a lock with a 60-second lifetime but keep renewing it automatically
# to ensure the lock is held for as long as the Python process is running.
with redis_lock.Lock(conn, name='my-lock', expire=60, auto_renewal=True):
    print 'please wait 10s'
    time.sleep(15)


# second
# On application start/restart
# import redis_lock
# redis_lock.reset_all(conn)

