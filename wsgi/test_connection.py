from bson.objectid import ObjectId

from wsgi.rr_people.queue import CommentQueue
from wsgi.rr_people.storage import CommentsStorage


def create_test_comment(sub="ts", text="test_text", fn="test_fn", link="http://test_link"):
    cs = CommentsStorage("test")

    text_hash = hash(sub + text + fn + link)
    cs.comments.delete_one({"fullname": fn, "text_hash": text_hash})
    result = cs.set_comment_info_ready(fn, text_hash, sub, text, link)

    assert result.inserted_id
    comment_id = str(result.inserted_id)

    found = cs.comments.find_one({"_id": ObjectId(comment_id)})
    assert found.get("_id") == result.inserted_id
    return comment_id, sub


if __name__ == '__main__':
    queue = CommentQueue("test", clear=True)

    cid, sub = create_test_comment()
    queue.put_comment(sub, cid)

    cid, sub = create_test_comment(sub="tst")
    queue.put_comment(sub, cid)

    cid, sub = create_test_comment(text="foo_bar_baz")
    queue.put_comment(sub, cid)

    cid, sub = create_test_comment(fn="fuckingfullname")
    queue.put_comment(sub, cid)

    cid, sub = create_test_comment(link="huipizdadjugurda")
    queue.put_comment(sub, cid)
