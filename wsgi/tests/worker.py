from Queue import Queue

from wsgi.rr_people.comment_suppliers.reddit import RedditCommentSupplier
from wsgi.rr_people.comment_suppliers.youtube import YoutubeCommentsSupplier
from wsgi.rr_people.reader import CommentSearcherWorker



def test_worker():
    csw = CommentSearcherWorker(Queue(), "videos", [YoutubeCommentsSupplier(), RedditCommentSupplier()])
    csw.start()


if __name__ == '__main__':
    test_worker()
