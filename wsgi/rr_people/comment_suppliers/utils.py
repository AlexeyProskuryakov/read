from collections import defaultdict
from threading import RLock

import time
from wsgi.rr_people import Singleton
from praw.objects import MoreComments


def comments_sequence(comments):
    """
    Getting generator of all first level comments
    :param self:
    :param comments:
    :return:
    """
    sequence = list(comments)
    position = 0
    while 1:
        to_add = []
        for i in xrange(position, len(sequence)):
            position = i
            comment = sequence[i]
            if isinstance(comment, MoreComments):
                to_add = comment.comments()
                break
            else:
                yield comment

        if to_add:
            sequence.pop(position)
            for el in reversed(to_add):
                sequence.insert(position, el)

        if position >= len(sequence) - 1:
            break


class CommentsInPost(object):
    __metaclass__ = Singleton
    TTL = 3600

    def __init__(self):
        self.mu = RLock()
        self._post_comments_cache = defaultdict(set)
        self._post_comments_cache_timings = {}

    def get_comments_of_post(self, post, cached=True):
        if cached:
            cached_comments = None
            with self.mu:
                if post.fullname in self._post_comments_cache and len(
                        self._post_comments_cache[post.fullname]) == post.num_comments:
                    if self._post_comments_cache_timings.get(post.fullname) - time.time() < self.TTL:
                        cached_comments = self._post_comments_cache.get(post.fullname)
                    else:
                        del self._post_comments_cache[post.fullname]

            if cached_comments:
                for comment in cached_comments:
                    yield comment
                return
        print "start comment seq", post.fullname, cached
        for comment in comments_sequence(post.comments):
            if cached:
                with self.mu:
                    self._post_comments_cache[post.fullname].add(comment)
                    self._post_comments_cache_timings[post.fullname] = time.time()

            yield comment
        print "end comment seq", post.fullname


if __name__ == '__main__':
    cip = CommentsInPost()
    cip2 = CommentsInPost()

    from wsgi.rr_people import RedditHandler

    r = RedditHandler()
    posts = r.get_hot_and_new("cringe", limit=10)
    post = posts[0]

    comments = list(cip.get_comments_of_post(post))
    comments2 = list(cip2.get_comments_of_post(post))

    assert comments == comments2

    comments_ = list(cip2.get_comments_of_post(posts[1]))
    comments_2 = list(cip.get_comments_of_post(posts[1]))

    assert comments_ == comments_2

    comments__ = list(cip.get_comments_of_post(posts[2], cached=False))
    comments__2 = list(cip.get_comments_of_post(posts[2], cached=False))

    assert comments__ == comments__2
