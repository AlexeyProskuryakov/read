import logging

import redis

from wsgi import ConfigManager

log = logging.getLogger("pq")

QUEUE_CF = lambda x: "cf_queue_%s" % x
NEED_COMMENT = "need_comment"


class CommentQueue():
    def __init__(self, name="?", clear=False, max_connections=2):
        cm = ConfigManager()
        self.redis = redis.StrictRedis(host=cm.get('comment_redis_address'),
                                       port=int(cm.get('comment_redis_port')),
                                       password=cm.get('comment_redis_password'),
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

    def put_comment(self, sbrdt, comment_id):
        log.debug("redis: push to %s %s" % (sbrdt, comment_id))
        self.redis.rpush(QUEUE_CF(sbrdt), comment_id)

    def get_all_comments_post_ids(self, sbrdt):
        result = self.redis.lrange(QUEUE_CF(sbrdt), 0, -1)
        return list(result)


if __name__ == '__main__':
    cq = CommentQueue(clear=True)
