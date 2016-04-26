import logging

import redis

from wsgi.properties import cfs_redis_port, cfs_redis_address, cfs_redis_password
from wsgi.rr_people.states import State

log = logging.getLogger("state_persist")

HASH_STATES = "states"
STATE = lambda x: "state_%s" % x


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
            pipe.set(STATE(aspect), state, ex=2)
        pipe.execute()

    def get_state(self, aspect):
        return State(self.redis.hget(HASH_STATES, aspect), self.redis.get(STATE(aspect)))