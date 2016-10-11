import datetime
import logging
from multiprocessing.process import current_process

import pymongo

from wsgi.db import DBHandler
from wsgi.properties import states_db_name, states_conn_url

from states.processes import ProcessDirector

log = logging.getLogger("state_persist")

HASH_STATES = "STATES"
STATE = lambda x: "state_%s" % (x)

STATE_TASK = "STATE_TASKS"

class StateObject(object):
    def __init__(self, global_state, mutex_state=None, history=None):
        self.global_state = global_state
        self.mutex_state = mutex_state
        self.history = history or []


class ProcessStatesPersist(ProcessDirector, DBHandler):
    def __init__(self, name="?", clear=False):
        DBHandler.__init__(self, "state persist %s" % name, uri=states_conn_url, db_name=states_db_name)
        ProcessDirector.__init__(self, "state_persist %s" % name)

        log.info("State persist [ %s ] inited for [%s]" % (states_conn_url, name))
        try:
            self.state_data = self.db.create_collection("state_data", capped=True, max=1000, size=1024*40)
            self.state_data.create_index("aspect")
            self.state_data.create_index([("time", pymongo.DESCENDING)], background=True)
        except Exception as e:
            self.state_data = self.db.get_collection("state_data")

    def get_process_state(self, aspect, history=False):
        global_state = self.redis.hget(HASH_STATES, aspect)
        mutex_state = super(ProcessStatesPersist, self).is_aspect_work(aspect, timing_check=False)
        result = StateObject(global_state, mutex_state)
        if history:
            result.history = self.get_state_data(aspect)

        return result

    def set_state_data(self, aspect, data):
        self.state_data.insert_one(
            dict({"aspect": aspect, "time": datetime.datetime.now(), "by": current_process().pid}, **data))
        self.redis.hset(HASH_STATES, aspect, data.get("state", "unknown"))

    def clear(self, aspect):
        self.state_data.delete_many({"aspect": aspect})

    def get_state_data(self, aspect):
        return list(self.state_data.find({"aspect": aspect}).sort("time", -1))

