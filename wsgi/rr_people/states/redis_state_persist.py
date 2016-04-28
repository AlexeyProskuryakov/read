import json
import logging

import redis

from wsgi.properties import cfs_redis_port, cfs_redis_address, cfs_redis_password
from wsgi.rr_people.states import StateObject, HeartBeatTask

log = logging.getLogger("state_persist")

HASH_STATES = "states"
STATE = lambda x: "state_%s" % x

PS_STATES = "PS_STATES"

EX_TIME = 2

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

        log.info("Redis state persist inited for [%s]" % name)

    def set_state(self, aspect, state):
        pipe = self.redis.pipeline()
        pipe.hset(HASH_STATES, aspect, state)
        pipe.set(STATE(aspect), state, ex=2)
        pipe.execute()

    def set_states(self, states):
        pipe = self.redis.pipeline()
        for aspect,state in states.iteritems():
            pipe.hset(HASH_STATES, aspect, state)
            pipe.set(STATE(aspect), state, ex=EX_TIME)
        pipe.execute()

    def get_state(self, aspect):
        return StateObject(self.redis.hget(HASH_STATES, aspect), self.redis.get(STATE(aspect)))

    def set_state_task(self, hb_task):
        self.redis.publish(PS_STATES, json.dumps(hb_task.to_dict()))

    def subscribe_state_tasks(self):
        pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(PS_STATES)
        for el in pubsub.listen():
            data = el.get('data')
            if data:
                data = json.loads(data)
                yield HeartBeatTask.from_dict(data)