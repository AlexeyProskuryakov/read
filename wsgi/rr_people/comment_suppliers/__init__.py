# coding=utf-8

class Supplier(object):
    def set_exclude_words(self, validation):
        raise NotImplementedError()

    def get_comment(self, post_acceptor):
        raise NotImplementedError()

    def get_name(self):
        raise NotImplementedError()

class CommentMainData(object):
    def __init__(self, post_fullname, post_permalink, text, hash):
        self.fullname = post_fullname
        self.post_url = post_permalink
        self.text_hash = hash
        self.text = text

    def get_data(self):
        return self.__dict__