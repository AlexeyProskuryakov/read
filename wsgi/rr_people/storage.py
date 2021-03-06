import json
import logging

import redis
import time

from bson.objectid import ObjectId

from wsgi import ConfigManager
from wsgi.db import DBHandler
from wsgi.rr_people import hash_word, START_TIME, END_TIME, LOADED_COUNT, CURRENT, PROCESSED_COUNT, IS_ENDED, IS_STARTED

CS_COMMENTED = "commented"
CS_READY_FOR_COMMENT = "ready_for_comment"

_comments = "comments"


class CommentsStorage(DBHandler):
    def __init__(self, name="?"):
        cm = ConfigManager()
        super(CommentsStorage, self).__init__(name=name,
                                              uri=cm.get('comments_mongo_uri'),
                                              db_name=cm.get('comments_db_name')
                                              )
        collections_names = self.db.collection_names(include_system_collections=False)
        if _comments not in collections_names:
            self.comments = self.db.create_collection(_comments)
            self.comments.create_index([("time", 1)])
            self.comments.create_index([("text_hash", 1)], unique=True)
            self.comments.create_index([("fullname", 1)])
            self.comments.create_index([("state", 1)], sparse=True)
            self.comments.create_index([("sub", 1)], sparse=True)
        else:
            self.comments = self.db.get_collection(_comments)

        words_exclude = "words_exclude"
        if words_exclude not in collections_names:
            self.words_exclude = self.db.create_collection(words_exclude)
            self.words_exclude.create_index([("hash", 1)], unique=True)
        else:
            self.words_exclude = self.db.get_collection(words_exclude)

    def set_words_exclude(self, new_words):
        new_words_hashes = dict(map(lambda x: (hash_word(x), x), new_words))
        for old_word in self.words_exclude.find({}, projection={"hash": 1}):
            if old_word['hash'] not in new_words_hashes:
                self.words_exclude.delete_one({"hash": old_word["hash"]})
            else:
                del new_words_hashes[old_word['hash']]

        if new_words_hashes:
            to_insert = map(lambda x: {"hash": x[0], "raw": x[1]},
                            [(hash, raw) for hash, raw in new_words_hashes.iteritems()])

            self.words_exclude.insert_many(to_insert, ordered=False)

    def get_words_exclude(self):
        return dict(map(lambda x: (x['hash'], x['raw']), self.words_exclude.find()))

    def add_ready_comment(self, comment_main_data, sub, supplier="reddit"):
        found = self.comments.find_one(
            {"$or": [
                {"fullname": comment_main_data.fullname},
                {"text_hash": comment_main_data.text_hash}]
            })
        if found:
            return None

        return self.comments.insert_one(dict(
            comment_main_data.get_data(),
            **{
                "time": time.time(),
                "state": CS_READY_FOR_COMMENT,
                "sub": sub,
                "supplier": supplier,
            }
        )
        )

    def get_comments_of_sub(self, sub=None, state=CS_READY_FOR_COMMENT):
        q = {}
        if state:
            q['state'] = state
        if sub:
            q['sub'] = sub

        return list(self.comments.find(q))

    def get_comments(self, posts_ids):
        for el in self.comments.find({"_id": {"$in": map(lambda x: ObjectId(x), posts_ids)}},
                                     projection={"_id": True, "text": True, "fullname": True, "post_url": True,
                                                 "supplier": True}):
            yield el

    def set_comment_state(self, comment_id, state):
        return self.comments.update_one({"_id": ObjectId(comment_id)}, {"$set": {"state": state}})


log = logging.getLogger("storage")

PERSIST_STATE = lambda x: "load_state_%s" % x
PREV_START_TIME = "p_t_start"
PREV_END_TIME = "p_t_end"
PREV_LOADED_COUNT = "p_loaded_count"


def post_to_dict(post):
    return {
        "created_utc": post.created_utc,
        "fullname": post.fullname,
        "num_comments": post.num_comments,
    }


class CommentFounderStateStorage(object):
    def __init__(self, name="?", clear=False, max_connections=2):
        cm = ConfigManager()
        self.redis = redis.StrictRedis(host=cm.get('cfs_redis_address'),
                                       port=int(cm.get('cfs_redis_port')),
                                       password=cm.get('cfs_redis_password'),
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
