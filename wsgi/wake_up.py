import random
import string
import time
from multiprocessing import Process

import requests
from flask import logging

from wsgi.db import DBHandler
from wsgi.rr_people.states.processes import ProcessDirector

log = logging.getLogger("wake_up")


class WakeUpStorage(DBHandler):
    def __init__(self, name="?"):
        super(WakeUpStorage, self).__init__(name=name)
        self.urls = self.db.get_collection("wake_up")
        if not self.urls:
            self.urls = self.db.create_collection("wake_up")
            self.urls.create_index("url_hash", unique=True)

    def get_urls(self):
        return map(lambda x: x.get("url"), self.urls.find({}, projection={'_id': False, "url_hash": False}))

    def add_url(self, url):
        hash_url = hash(url)
        found = self.urls.find_one({"url_hash": hash_url})
        if not found:
            log.info("add new url [%s]" % url)
            self.urls.insert_one({"url_hash": hash_url, "url": url})


class WakeUp(Process):
    def __init__(self):
        super(WakeUp, self).__init__()
        self.store = WakeUpStorage("wake_up")
        self.pd = ProcessDirector()

    def run(self):
        if not self.pd.start_aspect("wake_up", self.pid):
            return

        while 1:
            try:
                for url in self.store.get_urls():
                    salt = ''.join(random.choice(string.lowercase) for _ in range(20))
                    addr = "%s/wake_up/%s" % (url, salt)

                    result = requests.post(addr)
                    if result.status_code != 200:
                        time.sleep(1)
                        log.info("send: [%s][%s] not work will trying next times..." % (addr,result.status_code))
                        continue
                    else:
                        log.info("send: [%s] OK" % addr)
                    time.sleep(10)

            except Exception as e:
                log.exception(e)
            time.sleep(3600)


if __name__ == '__main__':
    WakeUp().start()
