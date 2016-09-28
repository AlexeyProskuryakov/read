# coding=utf-8
import logging
import re
import time
from datetime import datetime
from multiprocessing import Process
from multiprocessing.process import current_process

from wsgi.properties import min_copy_count, \
    shift_copy_comments_part, min_donor_comment_ups, max_donor_comment_ups, \
    DEFAULT_LIMIT
from wsgi.rr_people import RedditHandler, cmp_by_created_utc, post_to_dict, S_WORK, check_on_exclude, LOADED_COUNT, \
    START_TIME, END_TIME, IS_ENDED, PROCESSED_COUNT
from wsgi.rr_people import normalize
from wsgi.rr_people.queue import CommentQueue
from wsgi.rr_people.states.persist import ProcessStatesPersist
from wsgi.rr_people.storage import CommentsStorage, CommentFounderStateStorage

log = logging.getLogger("reader")


def _so_long(created, min_time):
    return (datetime.now() - datetime.fromtimestamp(created)).total_seconds() > min_time


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


def start(state_persist, aspect):
    _aspect = cs_aspect(aspect)
    _pid = current_process().pid
    started = state_persist.start_aspect(_aspect, _pid)
    if started.get("started", False):
        state_persist.set_state_data(_aspect, {"state": "started", "by": _pid})
        return True
    return False


def stop(state_persist, aspect):
    _aspect = cs_aspect(aspect)
    state_persist.stop_aspect(_aspect)
    state_persist.set_state_data(_aspect, {"state": "stopped"})


class CommentSearcherWorker(Process, RedditHandler):
    def __init__(self, queue, sub):
        super(CommentSearcherWorker, self).__init__()
        RedditHandler.__init__(self, "comment search worker for %s" % sub)

        self.queue = queue

        self.state_persist = ProcessStatesPersist(name="comment_searcher_worker")
        self.state_storage = CommentFounderStateStorage(name="comment_searcher_worker")

        self.comment_queue = CommentQueue(name="comment_searcher_worker")
        self.comment_storage = CommentsStorage(name="comment_searcher_worker")

        self.sub = sub

    def _get_acceptor(self, posts):
        posts.sort(cmp_by_created_utc)
        half_avg = float(reduce(lambda x, y: x + y.num_comments, posts, 0)) / (len(posts) * 2)
        for post in posts:
            if not post.archived and post.num_comments < half_avg:
                return post

    def _get_post_copies(self, post):
        search_request = "url:\'%s\'" % post.url
        copies = list(self.reddit.search(search_request)) + [post]
        return list(copies)

    def _retrieve_interested_comment(self, copy, post):
        # prepare comments from donor to selection
        after = copy.num_comments / shift_copy_comments_part
        if not after:
            return None, None
        if after > 34:
            after = 34
        for i, comment in enumerate(self.comments_sequence(copy.comments)):
            if i < after:
                continue
            if comment.ups >= min_donor_comment_ups and comment.ups <= max_donor_comment_ups and post.author != comment.author:
                body = comment.body
                if body:
                    check, tokens = self._check_comment_text(comment.body, post)
                    if check:
                        return comment, hash(tuple(tokens))
        return None, None

    def _check_comment_text(self, text, post):
        """
        Checking in db, and by is good and found similar text in post comments.
        Similar it is when tokens (only words) have equal length and full intersection
        :param text:
        :param post:
        :return:
        """
        if is_good_text(text):
            ok, c_tokens = check_on_exclude(text, self.exclude_words)
            if not ok:
                return False, None
            for p_comment in self.get_all_comments(post):
                p_text = p_comment.body
                if is_good_text(p_text):
                    p_tokens = set(normalize(p_text))
                    if len(c_tokens) == len(p_tokens) and len(p_tokens.intersection(c_tokens)) == len(p_tokens):
                        log.info("found similar text [%s] in post %s" % (c_tokens, post.fullname))
                        return False, None
            self.clear_cache(post)
            return True, c_tokens

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

    def find_comment(self, sub):
        self.exclude_words = self.comment_storage.get_words_exclude()
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
                    comment, c_hash = None, None
                    for copy in copies:
                        if post and copy.subreddit != post.subreddit and copy.fullname != post.fullname:
                            comment, c_hash = self._retrieve_interested_comment(copy, post)
                            if comment:
                                log.info("Find comment: [%s]\n in post: [%s] (%s) at subreddit: [%s]" % (
                                    comment.body, post, post.fullname, sub))
                                break

                    if comment:
                        insert_result = self.comment_storage.add_ready_comment(post.fullname, c_hash, sub,
                                                                               comment.body, post.permalink)
                        if not insert_result:
                            log.warning("Found stored comment: [%s]\n in post: [%s] (%s) at subreddit: [%s]" % (
                                comment.body, post, post.fullname, sub))
                            continue
                        else:
                            self.state_persist.set_state_data(cs_aspect(sub), {"state": "found", "for": post.fullname})
                            yield str(insert_result.inserted_id)

            except Exception as e:
                log.exception(e)

        self.state_storage.set_ended(sub)

    def comment_retrieve_iteration(self, sub):
        start(self.state_persist, sub)

        log.info("Will start find comments for [%s]" % (sub))
        try:
            for comment_id in self.find_comment(sub):
                self.comment_queue.put_comment(sub, comment_id)
        except Exception as e:
            log.exception(e)

        stop(self.state_persist, sub)

        self.queue.put(self.pid)

    def run(self):
        self.comment_retrieve_iteration(self.sub)
