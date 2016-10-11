import logging
import os
from multiprocessing import Queue
from threading import Thread, RLock

from wsgi.rr_people.comment_suppliers.reddit import RedditCommentSupplier
from wsgi.rr_people.comment_suppliers.youtube import YoutubeCommentsSupplier
from wsgi.rr_people.queue import CommentQueue
from wsgi.rr_people.reader import CommentSearcherWorker
from states.processes import ProcessDirector

log = logging.getLogger("manage")

SUPPLIERS = {"reddit": RedditCommentSupplier(),
             "youtube": YoutubeCommentsSupplier()}


class CommentSearcher():
    def __init__(self):
        """
        :param user_agent: for reddit non auth and non oauth client
        :param lcp: low copies posts if persisted
        :param cp:  commented posts if persisted
        :return:
        """
        self.end_queue = Queue()
        self.mu = RLock()
        self._processes = {}

        def start():
            log.info("Start supplying comments")
            comment_queue = CommentQueue(name="need_comments_supplier")
            for message in comment_queue.get_who_needs_comments():
                try:
                    nc_sub = message.get("data")
                    csw = CommentSearcherWorker(self.end_queue, nc_sub, SUPPLIERS)
                    csw.start()
                    log.info("was started %s" % csw.pid)
                    self.set_pid_data(csw.pid, csw)
                except Exception as e:
                    log.error("error at start comment search worker %s" % e)

        def end():
            while 1:
                pid = self.end_queue.get()
                log.info("will stopping %s" % pid)
                self.join_pid(pid)

        pd = ProcessDirector("comment searcher")
        if not pd.is_aspect_work("comment_searcher"):
            Thread(target=start).start()
            Thread(target=end).start()
            pd.start_aspect("comment_searcher", 5)
        else:
            log.info("Threads not start because another instance work")

        log.info("comment searcher inited!")

    def join_pid(self, pid):
        self.mu.acquire()
        self._processes[pid].join()
        del self._processes[pid]
        self.mu.release()

    def set_pid_data(self, pid, data):
        self.mu.acquire()
        self._processes[pid] = data
        self.mu.release()
