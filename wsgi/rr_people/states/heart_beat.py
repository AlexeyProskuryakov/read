import logging
import time
from Queue import Empty
from multiprocessing import Queue, Process
from multiprocessing.process import current_process
from subprocess import check_output
from threading import Thread

import signal

from wsgi.properties import HEART_BEAT_PERIOD, HEART_BEAT_PIDS_QUERY
from wsgi.rr_people import Singleton, S_WORK
from wsgi.rr_people.states import HeartBeatTask, AspectState
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
            while 1:
                for hb_task in self.state_persist.get_state_tasks():
                    log.info("found task in sp: %s" % hb_task)
                    self.queue.put(hb_task)
                time.sleep(5)

    def __init__(self, period=HEART_BEAT_PERIOD):
        super(HeartBeatManager, self).__init__()
        self.state_persist = StatePersist("heart beat")
        self.queue = Queue()
        self.qf = self.QueueFiller(self.queue, self.state_persist)

        self.aspect_states = {}
        self.pids = {}
        self.heart_beat_period = period

        log.info("inited")
        self.start()

    def wait_for_start(self):
        while not self.state_persist.start_hb_flag(self.pid):
            log.info("wait for start...")
            time.sleep(self.heart_beat_period)
        self.aspect_states[_me_aspect] = AspectState(_me_aspect, S_WORK, self.pid)

    def _del_aspect(self, aspect, pid):
        log.info("del aspect %s " % (aspect))
        del self.aspect_states[aspect]
        del self.pids[pid]

    def _add_aspect(self, aspect, pid, state):
        log.info("add %s with state %s" % (aspect, state))
        self.aspect_states[aspect] = state
        self.pids[pid] = aspect

    def process_tasks(self):
        tasks = []
        while 1:
            try:
                hb_task = self.queue.get_nowait()
                log.info("get task from queue: %s", hb_task)
                tasks.append(hb_task)
            except Empty as e:
                break
            except Exception as e:
                log.exception(e)
                break
        for hb_task in tasks:
            if hb_task and isinstance(hb_task, HeartBeatTask):
                if hb_task.action == HBS_ADD_ASPECT:
                    self._add_aspect(hb_task.aspect, hb_task.pid, hb_task.state)
                elif hb_task.action == HBS_REMOVE_ASPECT:
                    self._del_aspect(hb_task.aspect, hb_task.pid)

    def clear_dead(self):
        worked_pids = set(map(int, check_output(["pidof", HEART_BEAT_PIDS_QUERY]).split()))
        dead_pids = set(self.pids.keys()).difference(worked_pids)
        if dead_pids:
            self.state_persist.del_pids(dead_pids)
            for pid in dead_pids:
                aspect = self.pids[pid]
                log.info("will delete %s %s" % (aspect, pid))
                self._del_aspect(aspect, pid)

    def run(self):
        self.wait_for_start()
        self.qf.start()
        log.info("will start")
        while 1:
            self.process_tasks()
            self.clear_dead()

            if self.aspect_states:
                log.info("States of [%s]: \n %s" % (
                    self.pid, " | ".join(["%s:%s" % (k, v) for k, v in self.aspect_states.iteritems()])))

                self.state_persist.set_states(self.aspect_states, self.pid)

            time.sleep(self.heart_beat_period)

    def start_heart_beat(self, aspect, state, pid):
        log.info("start heart beat [%s] %s {%s}" % (aspect, state, pid))
        self.state_persist.set_state_task(HeartBeatTask(HBS_ADD_ASPECT, aspect, state, pid))

    def stop_heart_beat(self, aspect):
        log.info("stop heart beat %s" % aspect)
        self.state_persist.set_state_task(HeartBeatTask(HBS_REMOVE_ASPECT, aspect, None, None))


if __name__ == '__main__':
    hbm = HeartBeatManager()

    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    def f():
        log.info("will start...%s" % (current_process().pid))
        i = 0
        while i < 10:
            time.sleep(1)
            i += 1

        log.info("end...")

    p = Process(target=f)
    p.daemon=True
    p.start()


    hbm.start_heart_beat("test", S_WORK, p.pid)

    hbm.join()
