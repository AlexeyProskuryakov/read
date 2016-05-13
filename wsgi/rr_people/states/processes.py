# coding:utf-8
import logging
from multiprocessing.synchronize import Lock

import redis

from wsgi.properties import cfs_redis_address, cfs_redis_port, cfs_redis_password
from wsgi.rr_people.states import get_worked_pids

log = logging.getLogger("process_director")

PREFIX = lambda x: "PD_%s" % x
PREFIX_QUERY = "PD_*"
PREFIX_GET_DATA = lambda x: x.replace("PD_", "") if isinstance(x, (str, unicode)) and x.count("PD_") == 1 else x


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
        """
        starting or returning False if aspect already started
        :param aspect:
        :param pid:
        :return:
        """
        with self.mutex:
            result = self.redis.setnx(PREFIX(aspect), pid)
            if not result:
                aspect_pid = int(self.redis.get(PREFIX(aspect)))
                if aspect_pid in get_worked_pids():
                    return {"state": "already work", "by": aspect_pid, "started": False}
                else:
                    p = self.redis.pipeline()
                    p.delete(PREFIX(aspect))
                    p.set(PREFIX(aspect), pid)
                    p.execute()
                    return {"state": "restarted", "started": True}

            return {"state": "started", "started": True}

    def stop_aspect(self, aspect):
        self.redis.delete(PREFIX(aspect))

    def get_states(self):
        keys = self.redis.keys(PREFIX_QUERY)
        result = []
        if keys:
            worked_pids = get_worked_pids()
            for key in keys:
                pid = int(self.redis.get(key))
                result.append({"aspect": PREFIX_GET_DATA(key), "pid": pid, "work": pid in worked_pids})

    def get_state(self, aspect, worked_pids=None):
        pid_raw = self.redis.get(PREFIX(aspect))
        result = {"aspect": aspect,}
        wp = worked_pids or get_worked_pids()
        if pid_raw:
            pid = int(pid_raw)
            result = dict(result, **{"pid": pid, "work": pid in wp})
        else:
            result["work"] = False
        return result


if __name__ == '__main__':
    pd = ProcessDirector()
    pd.start_aspect("test", 1)
    pd.start_aspect("test", 1)
