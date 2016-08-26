from wsgi.db import DBHandler
from wsgi.properties import comments_mongo_uri, comments_db_name
from wsgi.rr_people import hash_word

CS_COMMENTED = "commented"
CS_READY_FOR_COMMENT = "ready_for_comment"

_comments = "comments"


class CommentsStorage(DBHandler):
    def __init__(self, name="?"):
        super(CommentsStorage, self).__init__(name=name, uri=comments_mongo_uri, db_name=comments_db_name)
        collections_names = self.db.collection_names(include_system_collections=False)
        if _comments not in collections_names:
            self.comments = self.db.create_collection(_comments)
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

    def set_comment_info_ready(self, post_fullname, text_hash, sub, comment_text, post_url):
        found = self.comments.find_one({"fullname": post_fullname, "text_hash": text_hash})
        if found:
            return None
        return self.comments.insert_one(
            {
                "fullname": post_fullname,
                "text_hash": text_hash,
                "state": CS_READY_FOR_COMMENT,
                "sub": sub,
                "text": comment_text,
                "post_url": post_url
            }
        )

    def get_posts_ready_for_comment(self, sub=None):
        q = {"state": CS_READY_FOR_COMMENT, "sub": sub}
        return list(self.comments.find(q))

    def get_posts_commented(self, sub):
        q = {"state": CS_COMMENTED, "sub": sub}
        return list(self.comments.find(q).sort([("time", -1)]))

    def get_posts(self, posts_fullnames):
        for el in self.comments.find({"fullname": {"$in": posts_fullnames}},
                                     projection={"text": True, "fullname": True, "post_url": True}):
            yield el
