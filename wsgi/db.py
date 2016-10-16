# coding=utf-8
import hashlib
import logging

from pymongo import MongoClient

from wsgi import ConfigManager

__author__ = 'alesha'

log = logging.getLogger("DB")


class DBHandler(object):
    def __init__(self, name="?", uri=None, db_name=None):
        cm = ConfigManager()
        uri = uri or cm.get("mongo_uri")
        db_name = db_name or cm.get("db_name")

        self.client = MongoClient(host=uri, maxPoolSize=10, connect=False)
        self.db = self.client[db_name]
        self.collection_names = self.db.collection_names(include_system_collections=False)

        log.info("start db handler for [%s] [%s/%s]" % (name, uri, db_name))


class HumanStorage(DBHandler):
    def __init__(self, name="?"):
        super(HumanStorage, self).__init__(name=name)
        db = self.db
        self.users = db.get_collection("users")
        if not self.users:
            self.users = db.create_collection()
            self.users.create_index([("name", 1)], unique=True)
            self.users.create_index([("user_id", 1)], unique=True)

        self.human_config = db.get_collection("human_config")
        if not self.human_config:
            self.human_config = db.create_collection("human_config")
            self.human_config.create_index([("user", 1)], unique=True)


    def get_all_humans_subs(self):
        cfg = self.human_config.find({}, projection={"subs":1})
        subs = []
        for el in cfg:
            subs.extend(el.get("subs", []))
        return list(set(subs))

        #######################USERS

    def add_user(self, name, pwd, uid):
        log.info("add user %s %s %s" % (name, pwd, uid))
        if not self.users.find_one({"$or": [{"user_id": uid}, {"name": name}]}):
            m = hashlib.md5()
            m.update(pwd)
            crupt = m.hexdigest()
            self.users.insert_one({"name": name, "pwd": crupt, "user_id": uid})

    def change_user(self, name, old_p, new_p):
        if self.check_user(name, old_p):
            m = hashlib.md5()
            m.update(new_p)
            crupt = m.hexdigest()
            self.users.insert_one({"name": name, "pwd": crupt})

    def check_user(self, name, pwd):
        found = self.users.find_one({"name": name})
        if found:
            m = hashlib.md5()
            m.update(pwd)
            crupt = m.hexdigest()
            if crupt == found.get("pwd"):
                return found.get("user_id")
