# coding=utf-8
import logging
import time
from datetime import datetime
from multiprocessing import Process
from multiprocessing.process import current_process

from wsgi.properties import DEFAULT_LIMIT
from wsgi.rr_people import RedditHandler, cmp_by_created_utc, S_WORK, LOADED_COUNT, START_TIME, END_TIME, IS_ENDED, \
    PROCESSED_COUNT
from wsgi.rr_people.entity_states import ProcessStatesPersist
from wsgi.rr_people.queue import CommentQueue
from wsgi.rr_people.storage import CommentsStorage, CommentFounderStateStorage, post_to_dict

log = logging.getLogger("reader")

MAX_POSTS_PER_SESSION = 10


def _so_long(created, min_time):
    return (datetime.now() - datetime.fromtimestamp(created)).total_seconds() > min_time


cs_aspect = lambda x: "CS_%s" % x


class CommentSearcherWorker(Process, RedditHandler):
    def __init__(self, queue, sub, suppliers):
        super(CommentSearcherWorker, self).__init__()
        RedditHandler.__init__(self, "comment search worker for %s" % sub)

        self.queue = queue

        self.state_persist = ProcessStatesPersist(name="comment_searcher_worker")
        self.state_storage = CommentFounderStateStorage(name="comment_searcher_worker")

        self.comment_queue = CommentQueue(name="comment_searcher_worker")
        self.comment_storage = CommentsStorage(name="comment_searcher_worker")

        self.sub = sub
        self.suppliers = suppliers

    def get_new_posts(self, sub):
        state = self.state_storage.get_state(sub)
        limit = DEFAULT_LIMIT
        if state:
            if state.get(IS_ENDED) == "True":
                end = float(state.get(END_TIME))
                start = float(state.get(START_TIME))
                loaded_count = float(state.get(LOADED_COUNT))

                _limit = ((time.time() - end) * loaded_count) / ((end - start) or 1.0)
                if _limit < 1:
                    _limit = 25
            else:
                _limit = int(state.get(LOADED_COUNT, DEFAULT_LIMIT)) - int(state.get(PROCESSED_COUNT, 0))

            limit = _limit if _limit < DEFAULT_LIMIT else DEFAULT_LIMIT

        posts = self.get_hot_and_new(sub, sort=cmp_by_created_utc, limit=limit)
        current = self.state_storage.get_current(sub)
        if current:
            posts = filter(lambda x: x.created_utc > current.get("created_utc"), posts)

        if len(posts):
            self.state_storage.persist_load_state(sub, posts[0].created_utc, posts[-1].created_utc, len(posts))
        return posts

    def find_comment(self, sub):
        posts = self.get_new_posts(sub)
        self.state_persist.set_state_data(cs_aspect(sub), {"state": S_WORK, "retrieved": len(posts)})
        self.state_storage.set_started(sub)
        log.info("Start finding comments to sub %s" % sub)
        count_saved = 0
        for post in posts:
            self.state_storage.set_current(sub, post_to_dict(post))
            try:
                for supplier in self.suppliers:
                    self.state_persist.set_state_data(cs_aspect(sub),
                                                      {"state": "start find", "supplier": supplier.get_name()})
                    supplier.set_exclude_words(self.comment_storage.get_words_exclude())
                    comment_main_data = supplier.get_comment(post)
                    if comment_main_data:
                        insert_result = self.comment_storage.add_ready_comment(comment_main_data,
                                                                               sub,
                                                                               supplier.get_name(),
                                                                               )
                        if not insert_result:
                            log.warning("Found already stored comment in post: [%s] at subreddit: [%s] :(" % (
                                post.fullname, sub))
                            continue
                        else:
                            log.info(
                                "Will store comment in post: [%s] at subreddit: [%s] :) [%s]" % (
                                    comment_main_data.fullname, sub, comment_main_data.text))
                            self.state_persist.set_state_data(cs_aspect(sub),
                                                              {"state": "found", "for": post.fullname})
                            yield str(insert_result.inserted_id)
                            count_saved += 1
                            if count_saved >= MAX_POSTS_PER_SESSION:
                                return
                            break

            except Exception as e:
                log.exception(e)

        self.state_storage.set_ended(sub)

    def comment_retrieve_iteration(self, sub):
        try:
            for comment_id in self.find_comment(sub):
                self.comment_queue.put_comment(sub, comment_id)
        except Exception as e:
            log.exception(e)

    def run(self):
        tracker = self.imply_start()

        self.comment_retrieve_iteration(self.sub)

        self.imply_stop()
        tracker.stop_track()

    def imply_start(self):
        _aspect = cs_aspect(self.sub)

        tracker = self.state_persist.start_aspect(_aspect)
        if tracker:
            self.state_persist.set_state_data(_aspect, {"state": "started", "by": current_process().pid})
            return tracker

    def imply_stop(self):
        _aspect = cs_aspect(self.sub)
        self.state_persist.set_state_data(_aspect, {"state": "stopped"})
        self.queue.put(self.pid)
