# coding=utf-8
import json
import logging
import re
import time
from datetime import datetime
from multiprocessing import Process
from multiprocessing.process import current_process

import redis

from wsgi.db import DBHandler
from wsgi.properties import min_copy_count, \
    shift_copy_comments_part, min_donor_comment_ups, max_donor_comment_ups, \
    comments_mongo_uri, comments_db_name, DEFAULT_LIMIT, cfs_redis_address, cfs_redis_port, cfs_redis_password
from wsgi.rr_people import RedditHandler, cmp_by_created_utc, post_to_dict, S_WORK, S_END
from wsgi.rr_people import normalize
from wsgi.rr_people.queue import CommentQueue
from wsgi.rr_people.states.persist import ProcessStatesPersist
from wsgi.rr_people.storage import CommentsStorage

log = logging.getLogger("reader")


def _so_long(created, min_time):
    return (datetime.utcnow() - datetime.fromtimestamp(created)).total_seconds() > min_time


PERSIST_STATE = lambda x: "load_state_%s" % x

START_TIME = "t_start"
END_TIME = "t_end"
LOADED_COUNT = "loaded_count"

PREV_START_TIME = "p_t_start"
PREV_END_TIME = "p_t_end"
PREV_LOADED_COUNT = "p_loaded_count"

IS_ENDED = "ended"
IS_STARTED = "started"
PROCESSED_COUNT = "processed_count"
CURRENT = "current"


class CommentFounderStateStorage(object):
    def __init__(self, name="?", clear=False, max_connections=2):
        self.redis = redis.StrictRedis(host=cfs_redis_address,
                                       port=cfs_redis_port,
                                       password=cfs_redis_password,
                                       db=0,
                                       max_connections=max_connections
                                       )
        if clear:
            self.redis.flushdb()

        log.info("Comment founder state storage for %s inited!" % name)

    def persist_load_state(self, sub, start, stop, count):
        p = self.redis.pipeline()

        key = PERSIST_STATE(sub)
        persisted_state = self.redis.hgetall(key)
        if persisted_state:
            p.hset(key, PREV_START_TIME, persisted_state.get(START_TIME))
            p.hset(key, PREV_END_TIME, persisted_state.get(END_TIME))
            p.hset(key, PREV_LOADED_COUNT, persisted_state.get(LOADED_COUNT))
            p.hset(key, PROCESSED_COUNT, 0)
            p.hset(key, CURRENT, json.dumps({}))

        p.hset(key, START_TIME, start)
        p.hset(key, END_TIME, stop)
        p.hset(key, LOADED_COUNT, count)
        p.execute()

    def set_ended(self, sub):
        p = self.redis.pipeline()
        p.hset(PERSIST_STATE(sub), IS_ENDED, True)
        p.hset(PERSIST_STATE(sub), IS_STARTED, False)
        p.execute()

    def set_started(self, sub):
        p = self.redis.pipeline()
        p.hset(PERSIST_STATE(sub), IS_ENDED, False)
        p.hset(PERSIST_STATE(sub), IS_STARTED, True)
        p.execute()

    def is_ended(self, sub):
        return self.redis.hget(PERSIST_STATE(sub), IS_ENDED)

    def is_started(self, sub):
        return self.redis.hget(PERSIST_STATE(sub), IS_STARTED)

    def set_current(self, sub, current):
        p = self.redis.pipeline()
        p.hset(PERSIST_STATE(sub), CURRENT, json.dumps(current))
        p.hincrby(PERSIST_STATE(sub), PROCESSED_COUNT, 1)
        p.execute()

    def get_current(self, sub):
        data = self.redis.hget(PERSIST_STATE(sub), CURRENT)
        if data:
            return json.loads(data)

    def get_proc_count(self, sub):
        return self.redis.hget(PERSIST_STATE(sub), PROCESSED_COUNT)

    def get_state(self, sub):
        return self.redis.hgetall(PERSIST_STATE(sub))

    def reset_state(self, sub):
        self.redis.hdel(PERSIST_STATE(sub), *self.redis.hkeys(PERSIST_STATE(sub)))
        return


cs_aspect = lambda x: "CS_%s" % x
is_cs_aspect = lambda x: x.count("CS_") == 1
cs_sub = lambda x: x.replace("CS_", "") if isinstance(x, (str, unicode)) and is_cs_aspect(x) else x

re_url = re.compile("((https?|ftp)://|www\.)[^\s/$.?#].[^\s]*")
re_crying_chars = re.compile("[A-Z]{2,}")
re_answer = re.compile("\> .*\n?")
re_slash = re.compile("/?r/\S+")
re_not_latin = re.compile("[^a-zA-Z0-9\,\.\?\!\:\;\(\)\$\%\#\@\-\+\=\_\/\\\\\"\'\[\]\{\}\>\<]\*\&\^\±\§\~\`")


def is_good_text(text):
    return len(text) >= 15 and \
           len(text) <= 140 and \
           len(re_not_latin.findall(text)) == 0 and \
           len(re_url.findall(text)) == 0 and \
           len(re_crying_chars.findall(text)) == 0 and \
           len(re_answer.findall(text)) == 0 and \
           len(re_slash.findall(text)) == 0


class CommentSearcher(RedditHandler):
    def __init__(self, user_agent=None):
        """
        :param user_agent: for reddit non auth and non oauth client
        :param lcp: low copies posts if persisted
        :param cp:  commented posts if persisted
        :return:
        """
        super(CommentSearcher, self).__init__(user_agent)
        self.comment_storage = CommentsStorage(name="comment_searcher")
        self.comment_queue = CommentQueue(name="comment_searcher")
        self.state_storage = CommentFounderStateStorage(name="comment_searcher")
        self.state_persist = ProcessStatesPersist(name="comment_searcher")
        self.processes = {}

        self.start_supply_comments()

        self._exclude_words = self.comment_storage.get_words_exclude()

        log.info("comment searcher inited!")

    def _start(self, aspect):
        _aspect = cs_aspect(aspect)
        _pid = current_process().pid
        started = self.state_persist.start_aspect(_aspect, _pid)
        if started.get("started", False):
            self.state_persist.set_state_data(_aspect, {"state": "started", "by": _pid})
            return True
        return False

    def _stop(self, aspect):
        _aspect = cs_aspect(aspect)
        self.state_persist.stop_aspect(_aspect)
        self.state_persist.set_state_data(_aspect, {"state": "stopped"})

    def comment_retrieve_iteration(self, sub):
        started = self._start(sub)
        if not started:
            log.info("Can not start comment retrieve iteration in [%s] because already started" % sub)
            return

        log.info("Will start find comments for [%s]" % (sub))
        try:
            for pfn in self.find_comment(sub):
                self.comment_queue.put_comment(sub, pfn)
        except Exception as e:
            log.exception(e)

        self._stop(sub)

    def start_comment_retrieve_iteration(self, sub):
        if sub in self.processes and self.processes[sub].is_alive():
            log.info("process for sub [%s] already work" % sub)
            return

        def f():
            self.comment_retrieve_iteration(sub)

        process = Process(name="csp [%s]" % sub, target=f)
        process.daemon = True
        process.start()
        self.processes[sub] = process

    def start_supply_comments(self):
        start = self._start("supply_comments")
        if not start:
            log.info("Can not supply because already supplied")
            return

        def f():
            log.info("Start supplying comments")
            for message in self.comment_queue.get_who_needs_comments():
                nc_sub = message.get("data")
                log.info("Receive need comments for sub [%s]" % nc_sub)
                self.start_comment_retrieve_iteration(nc_sub)

        process = Process(name="comment supplier", target=f)
        process.start()

    def _get_posts(self, sub):
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

    def _get_acceptor(self, posts):
        posts.sort(cmp_by_created_utc)
        half_avg = float(reduce(lambda x, y: x + y.num_comments, posts, 0)) / (len(posts) * 2)
        for post in posts:
            if not post.archived and post.num_comments < half_avg:
                return post

    def find_comment(self, sub, add_authors=False):
        posts = self._get_posts(sub)
        self.state_persist.set_state_data(cs_aspect(sub), {"state": S_WORK, "retrieved": len(posts)})
        self.state_storage.set_started(sub)
        log.info("Start finding comments to sub %s" % sub)
        for post in posts:
            self.state_storage.set_current(sub, post_to_dict(post))

            try:
                copies = self._get_post_copies(post)
                if len(copies) >= min_copy_count:
                    post = self._get_acceptor(copies)
                    comment = None
                    for copy in copies:
                        if post and copy.subreddit != post.subreddit and copy.fullname != post.fullname:
                            comment = self._retrieve_interested_comment(copy, post)
                            if comment:
                                log.info("Find comment: [%s]\n in post: [%s] (%s) at subreddit: [%s]" % (
                                    comment.body, post, post.fullname, sub))
                                break

                    if comment:
                        self.comment_storage.set_comment_info_ready(post.fullname, sub, comment.body, post.permalink)
                        self.state_persist.set_state_data(cs_aspect(sub), {"state": "found", "for": post.fullname})
                        yield post.fullname

            except Exception as e:
                log.exception(e)

        self.state_storage.set_ended(sub)

    def _get_post_copies(self, post):
        search_request = "url:\'%s\'" % post.url
        copies = list(self.reddit.search(search_request)) + [post]
        return list(copies)

    def _retrieve_interested_comment(self, copy, post):
        # prepare comments from donor to selection
        after = copy.num_comments / shift_copy_comments_part
        if not after:
            return
        if after > 34:
            after = 34
        for i, comment in enumerate(self.comments_sequence(copy.comments)):
            if i < after:
                continue
            if comment.ups >= min_donor_comment_ups and \
                            comment.ups <= max_donor_comment_ups and \
                            post.author != comment.author and \
                    self._check_comment_text(comment.body, post):
                return comment

    def _check_comment_text(self, text, post):
        """
        Checking in db, and by is good and found similar text in post comments.
        Similar it is when tokens (only words) have equal length and full intersection
        :param text:
        :param post:
        :return:
        """
        if is_good_text(text):
            c_tokens = set(normalize(text, lambda x: x))
            for token in c_tokens:
                if hash(token) in self._exclude_words:
                    return False

            for p_comment in self.get_all_comments(post):
                p_text = p_comment.body
                if is_good_text(p_text):
                    p_tokens = set(normalize(p_text, lambda x: x))
                    if len(c_tokens) == len(p_tokens) and len(p_tokens.intersection(c_tokens)) == len(p_tokens):
                        log.info("found similar text [%s] in post %s" % (c_tokens, post.fullname))
                        return False
            self.clear_cache(post)
            return True
