import logging

from wsgi.properties import min_donor_comment_ups, max_donor_comment_ups, min_copy_count
from wsgi.rr_people import RedditHandler, cmp_by_comments_count, cmp_by_created_utc
from wsgi.rr_people.comment_suppliers import Supplier, CommentMainData
from wsgi.rr_people.comment_suppliers.utils import CommentsInPost
from wsgi.rr_people.validate import check_comment_text, get_skip_comments_count

log = logging.getLogger("reddit_supplier")


def retrieve_interested_comment(after, another_comments, acceptor_author, acceptor_comments, exclude_words):
    # prepare comments from donor to selection
    for i, comment in enumerate(another_comments):
        if i < after:
            continue
        if comment.ups >= min_donor_comment_ups and comment.ups <= max_donor_comment_ups and acceptor_author != comment.author:
            check, tokens = check_comment_text(comment.body, acceptor_comments, exclude_words)
            if check:
                return comment, hash(tuple(tokens))
    return None, None


class RedditCommentSupplier(Supplier, RedditHandler):
    def __init__(self):
        super(RedditCommentSupplier, self).__init__()
        self.posts_comments_cache = {}

        self.exclude_words = {}
        self.cp = CommentsInPost()

    def comment_is_found(self, post):
        if post.fullname in self.posts_comments_cache:
            del self.posts_comments_cache[post.fullname]

    def set_exclude_words(self, validation):
        self.exclude_words = validation

    def get_comment(self, reddit_post):
        copies = self.get_not_archived_copies(reddit_post)
        if len(copies) >= min_copy_count:
            acceptor = self.get_post_with_less_comments(copies)
            log.info(
                "Will find comments for [%s][%s] in %s copies" % (acceptor.fullname, acceptor.permalink, len(copies)))
            try:
                acceptor_comments = self.cp.get_comments_of_post(acceptor)
                for copy in copies:
                    if acceptor and copy.subreddit != acceptor.subreddit and copy.fullname != acceptor.fullname:
                        after = get_skip_comments_count(copy.num_comments)
                        comment, c_hash = retrieve_interested_comment(
                            after,
                            self.cp.get_comments_of_post(copy, cached=False),
                            acceptor.author,
                            acceptor_comments,
                            self.exclude_words,
                        )
                        if comment:
                            return CommentMainData(acceptor.fullname, acceptor.permalink, comment.body, c_hash)

            except Exception as e:
                log.exception(e)

    def get_not_archived_copies(self, post):
        """
        :param reddit:
        :param post:
        :return:
        """
        search_request = "url:\'%s\'" % post.url
        copies = list(self.search(search_request)) + [post]
        copies = dict([(c.fullname, c) for c in copies]).values()
        return filter(lambda x: not x.archived, copies)

    def get_post_with_less_comments(self, posts):
        """
        Posts must be list of praw submissions
        :param posts:
        :return:
        """
        posts.sort(cmp_by_created_utc)
        half_avg = float(reduce(lambda x, y: x + y.num_comments, posts, 0)) / (len(posts) * 2)
        if half_avg < 1: half_avg = 2
        for post in posts:
            if post.num_comments < half_avg:
                return post

        posts.sort(cmp_by_comments_count)
        return posts[0]

    def get_name(self):
        return "reddit"
