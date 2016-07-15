from wsgi.db import DBHandler
from wsgi.properties import comments_mongo_uri, comments_db_name

CS_COMMENTED = "commented"
CS_READY_FOR_COMMENT = "ready_for_comment"

_comments = "comments"

class CommentsStorage(DBHandler):
    def __init__(self, name="?"):
        super(CommentsStorage, self).__init__(name=name, uri=comments_mongo_uri, db_name=comments_db_name)
        collections_names = self.db.collection_names(include_system_collections=False)
        if _comments not in collections_names:
            self.comments = self.db.create_collection(_comments)
            self.comments.create_index([("fullname", 1)])
            self.comments.create_index([("state", 1)], sparse=True)
            self.comments.create_index([("sub", 1)], sparse=True)
        else:
            self.comments = self.db.get_collection(_comments)

    def set_comment_info_ready(self, post_fullname, sub, comment_text, permalink):
        return self.comments.insert_one(
            {"fullname": post_fullname,
             "state": CS_READY_FOR_COMMENT,
             "sub": sub,
             "text": comment_text,
             "post_url": permalink}
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

