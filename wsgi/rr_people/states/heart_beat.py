import logging
import time
from multiprocessing import Queue
from threading import Thread

from wsgi.rr_people import Singleton
from wsgi.rr_people.states import HeartBeatTask
from wsgi.rr_people.states.redis_state_persist import StatePersist

HBS_ADD_ASPECT = "add"
HBS_REMOVE_ASPECT = "remove"

log = logging.getLogger("hart_beat")


class HeartBeatManager(Thread):
    __metaclass__ = Singleton

    def __init__(self, states_queue=None):
        super(HeartBeatManager, self).__init__()
        self.state_persist = StatePersist("heart beat")
        self.queue = states_queue or Queue()
        self.aspect_states = {}

    def run(self):
        while 1:
            try:
                hbelement = self.queue.get_nowait()
                if hbelement and isinstance(hbelement, HeartBeatTask):
                    log.info("have new aspect [%s] change [%s] with state [%s]" % (
                        hbelement.aspect, hbelement.action, hbelement.state))
                    if hbelement.action == HBS_ADD_ASPECT:
                        self.aspect_states[hbelement.aspect] = hbelement.state
                    elif hbelement.action == HBS_REMOVE_ASPECT:
                        del self.aspect_states[hbelement.aspect]
            except:
                pass

            if self.aspect_states:
                log.info("States: \n %s" % " | ".join(["%s:%s" % (k, v) for k, v in self.aspect_states.iteritems()]))
                self.state_persist.set_states(self.aspect_states)

            time.sleep(1)

    def set_state(self, aspect, state):
        self.queue.put(HeartBeatTask(HBS_ADD_ASPECT, aspect, state))

    def stop_heart_beat(self, aspect):
        self.queue.put(HeartBeatTask(HBS_REMOVE_ASPECT, aspect))
