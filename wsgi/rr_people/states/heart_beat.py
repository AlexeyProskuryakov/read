import logging
import time
from multiprocessing import Queue, Process
from threading import Thread

from wsgi.rr_people import Singleton, S_WORK
from wsgi.rr_people.states import HeartBeatTask
from wsgi.rr_people.states.redis_state_persist import StatePersist

HBS_ADD_ASPECT = "add"
HBS_REMOVE_ASPECT = "remove"

log = logging.getLogger("hart_beat")

_me_aspect = "heart_beat"


class HeartBeatManager(Process):
    __metaclass__ = Singleton

    class QueueFiller(Thread):
        def __init__(self, queue, state_persist):
            super(HeartBeatManager.QueueFiller, self).__init__()
            self.queue = queue
            self.state_persist = state_persist

        def run(self):
            for hb_task in self.state_persist.subscribe_state_tasks():
                log.info("qf have fill task %s" % hb_task)
                self.queue.put(hb_task)

    def __init__(self):
        super(HeartBeatManager, self).__init__()
        self.state_persist = StatePersist("heart beat")
        self.queue = Queue()
        self.qf = self.QueueFiller(self.queue, self.state_persist)

        self.aspect_states = {}
        log.info("inited")
        self.start()

    def is_started(self):
        log.info("checking started")
        time.sleep(2)
        state = self.state_persist.get_state(_me_aspect)
        if state.hb_state:
            log.info("already started")
            return True
        else:
            self.aspect_states[_me_aspect] = S_WORK
            return False

    def run(self):
        if self.is_started():
            return

        self.qf.start()
        log.info("start")
        while 1:
            try:
                hbelement = self.queue.get_nowait()
                if hbelement and isinstance(hbelement, HeartBeatTask):
                    log.info("have new aspect [%s] change [%s] with state [%s]" % (
                        hbelement.aspect, hbelement.action, hbelement.text_state))
                    if hbelement.action == HBS_ADD_ASPECT:
                        self.aspect_states[hbelement.aspect] = hbelement.text_state
                    elif hbelement.action == HBS_REMOVE_ASPECT:
                        del self.aspect_states[hbelement.aspect]
            except:
                pass

            if self.aspect_states:
                log.info("States: \n %s" % " | ".join(["%s:%s" % (k, v) for k, v in self.aspect_states.iteritems()]))
                self.state_persist.set_states(self.aspect_states)

            time.sleep(1)

    def start_heart_beat(self, aspect, state):
        self.state_persist.set_state_task(HeartBeatTask(HBS_ADD_ASPECT, aspect, state))

    def stop_heart_beat(self, aspect):
        self.state_persist.set_state_task(HeartBeatTask(HBS_REMOVE_ASPECT, aspect))


if __name__ == '__main__':
    hbm = HeartBeatManager()

    hbm.start_heart_beat("test", S_WORK)
    hbm.start_heart_beat("test2", S_WORK)

    hbm.join()
