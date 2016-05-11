import json
import logging

import redis

from wsgi.properties import cfs_redis_port, cfs_redis_address, cfs_redis_password, HEART_BEAT_PERIOD
from wsgi.rr_people.states import StateObject, HeartBeatTask, AspectState

log = logging.getLogger("state_persist")

HASH_STATES = "states"
STATE = lambda x: "state_%s" % (x)
PID = lambda x: "pid_%s" % x
PIDS = "pids"
STATES = "PS_STATES"

HB_WORK = "HB_WORK"


class StatePersist():
    def __init__(self, name="?", clear=False, max_connections=2):
        self.redis = redis.StrictRedis(host=cfs_redis_address,
                                       port=cfs_redis_port,
                                       password=cfs_redis_password,
                                       db=0,
                                       max_connections=max_connections
                                       )
        if clear:
            self.redis.flushdb()
        self.ex = HEART_BEAT_PERIOD * 2
        log.info("Redis state persist [%s] inited for [%s]" % (cfs_redis_address, name))

    def set_states(self, states, hb_process_pid=1):
        pipe = self.redis.pipeline()
        pipe.set(HB_WORK, hb_process_pid, ex=self.ex)
        for state in states:
            if isinstance(state, AspectState):
                pipe.hset(HASH_STATES, state.aspect, state.state)
                pipe.set(STATE(state.aspect), state.state, ex=self.ex)
                pipe.hset(PIDS, state.pid, state.aspect)
        pipe.execute()

    def get_state(self, aspect):
        global_state = self.redis.hget(HASH_STATES, aspect)
        hb_state = self.redis.get(STATE(aspect))
        return StateObject(global_state, hb_state)

    def get_pids(self):
        dead, work = [], []
        pids = self.redis.hgetall(PIDS)
        if pids:
            for pid, aspect in pids.iteritems():
                if self.redis.get(STATE(aspect)):
                    work.append(pid)
                else:
                    dead.append(pid)
        return dead, work

    def start_hb_flag(self, hb_process_pid=1):
        return self.redis.set(HB_WORK, hb_process_pid, nx=True, ex=self.ex)

    def set_state_task(self, hb_task):
        self.redis.lpush(STATES, json.dumps(hb_task.to_dict()))

    def get_all_pids(self):
        pids_dict = self.redis.hgetall(PIDS)
        return pids_dict

    def del_pids(self, pids):
        for pid in pids:
            aspect = self.redis.hget(PID, pid)
            self.redis.delete(STATE(aspect))
            self.redis.hdel(PID, pid)

    def get_state_tasks(self):
        while 1:
            raw_task = self.redis.rpop(STATES)
            if not raw_task:
                break
            task = HeartBeatTask.from_dict(json.loads(raw_task))
            yield task
