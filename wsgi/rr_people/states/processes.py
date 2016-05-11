# coding:utf-8
import logging
from multiprocessing.synchronize import Lock

import redis

from wsgi.properties import cfs_redis_address, cfs_redis_port, cfs_redis_password
from wsgi.rr_people.states import get_worked_pids

PREFIX = lambda x: "PD_%s" % x

log = logging.getLogger("process_director")


class ProcessDirector(object):
    def __init__(self, name="?", clear=False, max_connections=2):
        self.redis = redis.StrictRedis(host=cfs_redis_address,
                                       port=cfs_redis_port,
                                       password=cfs_redis_password,
                                       db=0,
                                       max_connections=max_connections
                                       )
        if clear:
            self.redis.flushdb()

        self.mutex = Lock()
        log.info("Inited")


    def start_aspect(self, aspect, pid):
        with self.mutex:
            result = self.redis.setnx(PREFIX(aspect), pid)
            if not result:
                aspect_pid = int(self.redis.get(PREFIX(aspect)))
                if aspect_pid in get_worked_pids():
                    return False
                else:
                    p = self.redis.pipeline()
                    p.delete(PREFIX(aspect))
                    p.set(PREFIX(aspect), pid)
                    p.execute()
                    return True

            return result

    def stop_aspect(self, aspect):
        self.redis.delete(PREFIX(aspect))


if __name__ == '__main__':
    pd = ProcessDirector()
    pd.start_aspect("test", 1)
    pd.start_aspect("test", 1)
