import datetime
import json
import logging

import pymongo

from wsgi.db import DBHandler
from wsgi.properties import cfs_redis_address, states_db_name, states_conn_url
from wsgi.rr_people import S_WORK, S_TERMINATED
from wsgi.rr_people.states import StateObject, HeartBeatTask
from wsgi.rr_people.states.processes import ProcessDirector

log = logging.getLogger("state_persist")

HASH_STATES = "STATES"
STATE = lambda x: "state_%s" % (x)

STATE_TASK = "STATE_TASKS"


class StatePersist(ProcessDirector, DBHandler):
    def __init__(self, name="?", clear=False, max_connections=2):
        ProcessDirector.__init__(self, "state persist %s" % name, clear, max_connections)
        DBHandler.__init__(self, "state persist %s" % name, uri=states_conn_url, db_name=states_db_name)

        log.info("State persist [ %s | %s ] inited for [%s]" % (cfs_redis_address, states_conn_url, name))
        try:
            self.state_data = self.db.create_collection("state_data", capped=True, max=1000)
            self.state_data.create_index("aspect")
            self.state_data.create_index([("time", pymongo.DESCENDING)], background=True)
        except Exception as e:
            self.state_data = self.db.get_collection("state_data")

    def set_state(self, aspect, state):
        return self.redis.hset(HASH_STATES, aspect, state)

    def get_state(self, aspect, history=False, worked_pids=None):
        global_state = self.redis.hget(HASH_STATES, aspect)
        pd_state = super(StatePersist, self).get_state(aspect, worked_pids=worked_pids)

        result = StateObject(global_state,
                             S_WORK if pd_state.get("work") else S_TERMINATED)
        if history:
            result.history = self.get_state_data(aspect)

        return result

    def set_state_data(self, aspect, data):
        self.state_data.insert_one(dict({"aspect": aspect, "time": datetime.datetime.utcnow()}, **data))

    def get_state_data(self, aspect):
        return list(self.state_data.find({"aspect": aspect}).sort("time", 1))

        # tasks...

    def set_state_task(self, hb_task):
        self.redis.lpush(STATE_TASK, json.dumps(hb_task.to_dict()))

    def get_state_tasks(self):
        while 1:
            raw_task = self.redis.rpop(STATE_TASK)
            if not raw_task:
                break
            task = HeartBeatTask.from_dict(json.loads(raw_task))
            yield task