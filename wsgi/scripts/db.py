import time

from wsgi.rr_people.queue import CommentQueue
from wsgi.rr_people.storage import CommentsStorage, CS_READY_FOR_COMMENT


def set_time_for_comments():
    storage = CommentsStorage("scripts")
    storage.comments.create_index([("time", 1)])
    result = storage.comments.update_many({"state": CS_READY_FOR_COMMENT, "time": {"$exists": False}},
                                 {"$set": {"time": time.time()}})
    print result.modified_count, result.matched_count


def load_comments_into_queue():
    storage = CommentsStorage("scripts")
    queue = CommentQueue("scripts")

    for comment in storage.comments.find({"state": CS_READY_FOR_COMMENT}):
        sub = comment.get("sub")
        c_id = str(comment.get("_id"))
        comment_ids = queue.get_all_comments_post_ids(sub)
        if c_id not in comment_ids:
            print "add comment in queue: %s" % comment
            queue.put_comment(sub, c_id)


if __name__ == '__main__':
    set_time_for_comments()
    load_comments_into_queue()
