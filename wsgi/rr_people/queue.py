import logging

import redis

from wsgi.properties import queue_redis_address, queue_redis_password, queue_redis_port

log = logging.getLogger("pq")

QUEUE_CF = lambda x: "cf_queue_%s" % x
NEED_COMMENT = "need_comment"


class CommentQueue():
    def __init__(self, name="?", clear=False, max_connections=2):
        self.redis = redis.StrictRedis(host=queue_redis_address,
                                       port=queue_redis_port,
                                       password=queue_redis_password,
                                       db=0,
                                       max_connections=max_connections
                                       )
        if clear:
            self.redis.flushdb()

        log.info("Production Queue inited for [%s]" % name)

    def need_comment(self, sbrdt):
        self.redis.publish(NEED_COMMENT, sbrdt)

    def get_who_needs_comments(self):
        pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(NEED_COMMENT)
        for el in pubsub.listen():
            yield el

    def put_comment(self, sbrdt, post_fn):
        log.debug("redis: push to %s %s" % (sbrdt, post_fn))
        self.redis.rpush(QUEUE_CF(sbrdt), post_fn)

    def pop_comment(self, sbrdt):
        result = self.redis.lpop(QUEUE_CF(sbrdt))
        log.debug("redis: get by %s\nthis: %s" % (sbrdt, result))
        return result

    def get_all_comments_post_ids(self, sbrdt):
        result = self.redis.lrange(QUEUE_CF(sbrdt), 0, -1)
        return list(result)


if __name__ == '__main__':
    cq = CommentQueue(clear=True)