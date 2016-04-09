# coding=utf-8
import logging
import random
from collections import defaultdict, Counter
from datetime import datetime
from multiprocessing import Process
from multiprocessing.queues import Queue
from random import choice

import praw
import pymongo
from pymongo.mongo_client import MongoClient

from wsgi.db import DBHandler
from wsgi.properties import \
    ae_mongo_uri, \
    ae_db_name, \
    DAY, HOUR, MINUTE, SEC, WEEK_DAYS, WEEK, \
    AE_MIN_COMMENT_KARMA, \
    AE_MIN_LINK_KARMA, \
    AE_MIN_SLEEP_TIME, \
    AE_MAX_SLEEP_TIME, \
    AE_AUTHOR_MIN_ACTIONS
from wsgi.rr_people import USER_AGENTS, A_SLEEP, A_CONSUME, A_COMMENT, A_POST

log = logging.getLogger("ae")

__doc__ = """
    По сути марковская цепь: где узлы есть временные точки, а связи - события произошедшие между этими точками, имеющие вес равный
    у скольких авторов эти события произошли. Ввсегда есть событие - ниего не делать.
    Определяет что делать чуваку в текущий момент: потреблять, комментировать, постить, ничего не делать.
    Для создания требуется мена авторов, по которым будет строиться модель.


    Строится в два этапа.
    Первый, выделение авторов поведение которых будет номиналом:
    1) При извлечении комментариев, выделяем авторов коментариев и авторов постов.
    Наполняем информацию об авторах таким образом: <час>;<день недели>:<автор>:<тип действия><количество>.
    2) В этой таблице делаем аггрегацию: автор:количество действий. Выбираем средне постящих или много постящих.
    3) В этой же таблице делаем аггрегацию по <час>, <день недели> и находим последовательности бездействия авторов (цепочки отсутствия).
    Это будет список [<час>, <день недели>] которые идут подряд и в которых нету какой-либо активности выбранных на 2 этапе авторов.
    Берем те списки которые длины от 2 до 8 часов.
    Метрикой класстеризации будет пересечение этих списков. Класстеризуем с использованием метрикии и получаем класстеры из авторов,
    сидящих примерно в одно и то же время.

    Второй этап получение инфорации об этих авторах.
    1) Строим модель на основе комментариев данных реддитом по авторам извлеченным на первом этапе. GET /user/username/comments
    То есть сохраняем каждый комментрарий так: <минута: час: день недели> : автор : количество. И делаем аггрегацию:
    <минута: час: день недели> : количество комментариев: количество авторов сделавших комментарий.
    Также можно и с воутами сделать.
    2) Модель отвечает что делать в определенную минуту часа дня недели. Высчитывая веса сколько авторов сделали комментарии а сколько
      в этот промежуток времени отсутсвовали.
      Отсутствие считаем тогда когда время попадает в цепочку отсутствия.
    3) В модель можно добавлять или удалять авторов.

    """

def hash_string(time_hash):
    if not isinstance(time_hash, int):
        return None
    else:
        d, r = divmod(time_hash, DAY)
        h, r = divmod(r, HOUR)
        m, r = divmod(r, MINUTE)
        s = r
        return "%s %s:%s:%s" % (WEEK_DAYS[d], h, m, s)


def delta_info(delta):
    if not isinstance(delta, int):
        return None
    else:
        d, r = divmod(delta, DAY)
        h, r = divmod(r, HOUR)
        m, r = divmod(r, MINUTE)
        s = r
        return "days: %s, hours: %s, minutes: %s, secs: %s" % (d, h, m, s)


def time_hash(time):
    if not isinstance(time, datetime):
        return ""
    else:
        h = time.hour
        m = time.minute
        s = time.second
        d = time.weekday()
        return d * DAY + h * HOUR + m * MINUTE + s * SEC


def weighted_choice_king(action_weights):
    total = 0
    winner = 0
    for i, w in action_weights.iteritems():
        total += w
        if random.random() * total < w:
            winner = i
    return winner


class AuthorsStorage(DBHandler):
    def __init__(self, name="?", mongo_uri=ae_mongo_uri, db_name=ae_db_name):
        super(AuthorsStorage, self).__init__(name=name, uri=mongo_uri, db_name=db_name)

        self.authors = self.db.get_collection("ae_authors")
        if not self.authors:
            self.authors = self.db.create_collection(
                    "ae_authors",
            )

            self.authors.create_index([("author", pymongo.ASCENDING)])
            self.authors.create_index([("action_type", pymongo.ASCENDING)])
            self.authors.create_index([("time", pymongo.ASCENDING)])
            self.authors.create_index([("end_time", pymongo.ASCENDING)])
            self.authors.create_index([("used", pymongo.ASCENDING)], sparse=True)

        self.author_groups = self.db.get_collection("ae_author_groups")

        if not self.author_groups:
            self.author_groups = self.db.create_collection("ae_author_groups")
            self.author_groups.create_index([("name", pymongo.ASCENDING)])

    def get_interested_authors(self, min_count_actions=AE_AUTHOR_MIN_ACTIONS):
        result = self.authors.aggregate([
            {"$match": {"used": {"$exists": False}}},
            {"$group": {"_id": "$author", "count": {"$sum": "$count"}}},
            {"$match": {"count": {"$gte": min_count_actions}}}])
        return filter(lambda x: x is not None, map(lambda x: x.get("_id"), result))

    def get_authors_groups(self, min_difference=2 * HOUR, difference_step=2 * HOUR, step_count=100, min_nights=5,
                           by=A_SLEEP, min_groups=2):
        authors = defaultdict(list)
        groups = []

        for author in self.get_interested_authors():
            author_steps = []
            for step in self.authors.find(
                    {"end_time": {"$exists": True}, "action_type": by, "author": author}).sort("time"):
                author_steps.append(dict(step))
            if len(author_steps) >= min_nights:
                authors[author] = author_steps[:]
                groups.append({author})

        if not authors:
            return None

        log.info("Preparing authors for groups: %s" % authors.keys())

        def create_new_groups(nearest_groups, groups):
            auth_1 = set(groups[nearest_groups[0]])
            auth_2 = set(groups[nearest_groups[1]])
            new_group = auth_1.union(auth_2)
            result = [new_group]
            for g_id, group in enumerate(groups):
                if g_id not in nearest_groups:
                    result.append(group)
            return result

        group_result = []
        step = 0

        while len(groups) > min_groups or len(groups) == 0:
            log.info("adding to group. Group len is:%s" % len(groups))
            max_nearest_weight = 0
            nearest_groups = (None, None)

            for i, authors_group in enumerate(groups):
                for j, authors_a_group in enumerate(groups):
                    if i == j: continue
                    nearest = 0.0
                    for author in authors_group:
                        for a_author in authors_a_group:
                            nearest += self.get_authors_nearest(authors[author], authors[a_author])

                    if nearest < 0:
                        continue

                    nearest = nearest / (len(authors_group) + len(authors_a_group))
                    if max_nearest_weight < nearest:
                        max_nearest_weight = nearest
                        nearest_groups = (i, j)

            if nearest_groups == (None, None):
                break

            groups = create_new_groups(nearest_groups, groups)

            group_result.append(groups[:])
            log.info("filtered groups count: %s" % len(groups))

            step += 1

            if step > step_count:
                break

        best_group = self.get_best_group(group_result[-1], authors)

        result = {'best': best_group}
        max_subst = WEEK
        for i, _ in enumerate(group_result):
            dg1, dg2, g_subst = self.get_max_difference_groups(group_result[i], authors)
            if g_subst < max_subst:
                max_subst = g_subst
                result['difference_1'] = dg1
                result['difference_2'] = dg2

        result['authors'] = authors
        result['all_groups'] = group_result
        return result

    def get_authors_nearest(self, author1_steps, author2_steps):
        """
        #todo go for each day and create result.
        :param author1_steps:
        :param author2_steps:
        :return: more is than authors more equals
        """
        result = 0
        s = lambda x: x['time']
        e = lambda x: x["end_time"]

        def get_author_steps(authors_steps, from_, to_):
            result = []
            for step in authors_steps:
                if s(step) >= from_ and s(step) <= to_:
                    result.append(step)
            return result

        for step in range(0, WEEK, DAY):
            step_result = 0
            a1stps = get_author_steps(author1_steps, step, step + DAY)
            a2stps = get_author_steps(author2_steps, step, step + DAY)

            for one in a1stps:
                for two in a2stps:

                    diff = abs(e(one) - e(two)) + abs(s(one) - s(two))
                    inter = min((e(two) - s(one)), (e(one) - s(two)))
                    union = max((e(two) - s(one)), (e(one) - s(two)))

                    # steps have not intersection
                    if inter <= 0:
                        step_result -= union
                        continue

                    step_result += inter - diff

            result += step_result

        return result

    def get_best_group(self, groups, authors):
        best_group = None
        max_group_nearest = 0
        for group in groups:
            if len(group) < 2: continue
            group_nearest = 0
            for author in group:
                for a_author in group:
                    if author == a_author: continue
                    group_nearest += self.get_authors_nearest(authors[author], authors[a_author])
            if group_nearest > max_group_nearest:
                max_group_nearest = group_nearest
                best_group = group

        return best_group

    def get_max_difference_groups(self, groups, authors):
        def subst_groups(group1, group2):
            nearest = 0
            for g1a in group1:
                for g2a in group2:
                    nearest += self.get_authors_nearest(authors[g1a], authors[g2a])
            return nearest

        min_nearest_groups = WEEK * 1000
        result = None
        for i, group in enumerate(groups):
            for j, a_group in enumerate(groups):
                if i == j: continue
                nearest = subst_groups(group, a_group)
                if nearest < min_nearest_groups:
                    min_nearest_groups = nearest
                    result = group, a_group, min_nearest_groups
        return result

    def set_group(self, authors, group_name):
        if not authors:
            return

        found = self.author_groups.find_one({"name": group_name})
        if not found:
            self.author_groups.insert_one({"name": group_name, "authors": authors})
        else:
            self.author_groups.update_one({"name": group_name}, {'$set': {"authors": authors}})
            self.authors.update_many({"author": {"$in": found.get("authors")}}, {"$unset": {"used", ""}})

        self.authors.update_many({"author": {"$in": authors}, "used":{"$ne":group_name}}, {"$set": {"used": group_name}})

    def get_sleep_steps(self, group):
        return list(self.authors.find({"used": group, "action_type": A_SLEEP}))

    def get_all_groups(self):
        return list(self.author_groups.find({}))


class ActionGeneratorDataFormer(object):
    class AuthorAdder(Process):
        def __init__(self, queue, outer):
            super(ActionGeneratorDataFormer.AuthorAdder, self).__init__(name="author_adder")
            self.q = queue
            self.outer = outer

        def run(self):
            log.info("author adder started")
            while 1:
                try:
                    author = self.q.get()
                    r_author = self.outer._get_author_object(author)
                    c_karma = r_author.__dict__.get("comment_karma", 0)
                    l_karma = r_author.__dict__.get("link_karma", 0)
                    if c_karma > AE_MIN_COMMENT_KARMA and l_karma > AE_MIN_LINK_KARMA:
                        log.info("will add [%s] for action engine" % (author))
                        self.outer._add_author_data(r_author)
                except Exception as e:
                    log.exception(e)

    def __init__(self):
        self._storage = AuthorsStorage("author_generator_data_former")
        self._r = praw.Reddit(user_agent=choice(USER_AGENTS))
        self._queue = Queue()

        adder = ActionGeneratorDataFormer.AuthorAdder(self._queue, self)
        adder.start()

    def is_author_added(self, author):
        found = self._storage.authors.find_one({"author": author})
        return found is not None

    def save_action(self, author, action_type, time, end_time=None):
        q = {"author": author, "action_type": action_type}
        if isinstance(time, datetime):
            q["time"] = time_hash(time)
        elif isinstance(time, int):
            q["time"] = time

        if end_time:
            q["end_time"] = end_time

        found = self._storage.authors.find_one(q)
        if found:
            self._storage.authors.update_one(q, {"$inc": {"count": 1}})
        else:
            q["count"] = 1
            self._storage.authors.insert_one(q)

    def revert_sleep_actions(self, group_id=None):
        q = {'end_time': {'$exists': True}}
        if group_id:
            q["used"] = group_id
        self._storage.authors.delete_many(q)

    def fill_consume_and_sleep(self, authors_min_actions_count=AE_AUTHOR_MIN_ACTIONS, min_sleep=AE_MIN_SLEEP_TIME,
                               max_sleep=AE_MAX_SLEEP_TIME):
        for author in self._storage.get_interested_authors(authors_min_actions_count):
            start_time, end_time = 0, 0
            actions = self._storage.authors.find({"author": author}).sort("time", 1)
            for i, action in enumerate(actions):
                if i == 0:
                    start_time = action.get("time")
                    continue

                end_time = action.get("time")
                delta = (end_time - start_time)
                if delta > min_sleep and delta < max_sleep:
                    self.save_action(author, A_SLEEP, start_time, end_time)
                start_time = end_time

            log.info("Was update consume and sleep steps for %s" % author)

    def _get_author_object(self, author_name):
        r_author = self._r.get_redditor(author_name, fetch=True)
        return r_author

    def _get_data_of(self, r_author):
        try:
            cb = list(r_author.gets(sort="new", limit=1000))
            sb = list(r_author.get_submitted(sort="new", limit=1000))
            return cb, sb
        except Exception as e:
            log.exception(e)
        return [], []

    def _add_author_data(self, r_author):
        log.info("will retrieve comments of %s" % r_author.name)
        _comments, _posts = self._get_data_of(r_author)
        for comment in _comments:
            self.save_action(r_author.name, A_COMMENT, datetime.fromtimestamp(comment.created_utc))

        for submission in _posts:
            self.save_action(r_author.name, A_POST, datetime.fromtimestamp(submission.created_utc))

        log.info("fill %s comments and %s posts" % (len(_comments), len(_posts)))

    def add_author_data(self, author):
        if not self.is_author_added(author):
            self._queue.put(author)


class ActionGenerator(object):
    class ActionStack():
        def __init__(self, size):
            self.data = []
            self.size = size

        def push(self, action):
            self.data.append(action)
            if len(self.data) > self.size:
                del self.data[0]

        def get_prevailing_action(self):
            if self.data:
                cnt = Counter(self.data)
                return cnt.most_common()[0][0]

        def __contains__(self, item):
            return item in self.data

    def __init__(self, group_name=None, size=5):
        self.group_name = group_name
        self._storage = AuthorsStorage("author generator")
        self._r = praw.Reddit(user_agent=choice(USER_AGENTS))
        self._action_stack = ActionGenerator.ActionStack(size)
        log.info("Activity engine inited!")

    def set_group_name(self, group_name):
        self.group_name = group_name

    def get_action(self, for_time, step=MINUTE):
        if not self.group_name:
            log.error("For action generator group name is not exists")
            return None
        pipe = [
            {"$match": {"used": self.group_name,
                        "$or": [{"time": {"$gte": for_time, "$lte": for_time + step}},
                                {"end_time": {"$gte": for_time + step}, "time": {"$lte": for_time}}
                                ]}
             },
            {"$group": {"_id": "$action_type", "count": {"$sum": "$count"}, "authors": {"$addToSet": "$author"}}}
        ]
        result = self._storage.authors.aggregate(pipe)
        action_weights = {}
        non_consumed_authors = []
        for r in result:
            action_weights[r.get("_id")] = r.get("count")
            non_consumed_authors.extend(r.get("authors"))

        getted_action = weighted_choice_king(action_weights)
        self._action_stack.push(getted_action)
        if A_SLEEP in self._action_stack:
            return self._action_stack.get_prevailing_action()
        else:
            return getted_action


def visualise_steps(groups, authors_steps):
    import matplotlib.pyplot as plt

    counter = 1
    clrs = ["b", "g", "r", "c", "m", "y", "k"]
    for i, group in enumerate(groups):
        if not group: continue
        c = random.choice(clrs)
        fstp = None
        for author in group:
            counter += 1
            for step in authors_steps[author]:
                if not fstp:
                    fstp = step
                plt.plot([step.get("time"), step.get("end_time")], [counter, counter], "k-", lw=1,
                         label=author,
                         color=c)

        plt.text(fstp["time"], counter, "%s" % i)

    plt.axis([0, WEEK, 0, len(authors_steps) + 5])
    plt.xlabel("time")
    plt.ylabel("authors")
    plt.show()


def renew_sleep_actions():
    agdf = ActionGeneratorDataFormer()
    agdf.revert_sleep_actions()
    agdf.fill_consume_and_sleep(min_sleep=4 * HOUR, max_sleep=24 * HOUR, authors_min_actions_count=0)


def group_and_visualise_gen(for_time=DAY * 2):
    ae = ActionGenerator()
    a_s = AuthorsStorage()

    g_res = a_s.get_authors_groups()

    visualise_steps([g_res.get("best"), g_res.get('difference_1'), g_res.get('difference_2')], g_res.get("authors"))
    # for group in g_res.get("all_groups"):
    #     visualise_steps(group, g_res.get("authors"))

    a_s.set_group(list(g_res.get('difference_1')), "Shlak2k15")
    a_s.set_group(list(g_res.get('difference_2')), "Shlak2k16")
    # a_s.set_group(g_res.get("best"), "best")

    import matplotlib.pyplot as plt

    count = defaultdict(int)
    for t in range(0, for_time, HOUR / 2):
        result = ae.get_action(t, HOUR / 2)
        if result == A_SLEEP:
            plt.plot([t], [2], "o", color="r")
            count[A_SLEEP] += 1
        if result == A_CONSUME:
            plt.plot([t], [4], "o", color="g")
            count[A_CONSUME] += 1
        if result == A_POST:
            plt.plot([t], [5], "o", color="m")
            count[A_POST] += 1
        if result == A_COMMENT:
            plt.plot([t], [6], "o", color="b")
            count[A_COMMENT] += 1

    print count

    plt.axis([0, for_time, 0, 8])
    plt.xlabel("time")
    plt.ylabel("actions")
    plt.show()


def create():
    a_s = AuthorsStorage()
    a_s.authors.delete_many({})

    from wsgi.rr_people.reader import CommentSearcher, ProductionQueue
    from wsgi.db import HumanStorage

    db = HumanStorage()
    cs = CommentSearcher(db, add_authors=True)
    cq = ProductionQueue()

    sbrdt = "videos"
    for post, comment in cs.find_comment(sbrdt):
        cq.put_comment(sbrdt, post, comment)

    agdf = ActionGeneratorDataFormer()
    agdf.fill_consume_and_sleep()


def copy_data(from_uri, from_db_name, to_uri, to_db_name, drop_dest=False):
    src_as = AuthorsStorage(name="from", mongo_uri=from_uri, db_name=from_db_name)
    dest_as = AuthorsStorage(name="to", mongo_uri=to_uri, db_name=to_db_name)
    if drop_dest:
        dest_as.authors.drop()
        dest_as.author_groups.drop()

    for group in src_as.authors.aggregate([{"$match": {"used": {"$exists": True}}}, {"$group": {"_id": "$used", "authors":{"$addToSet":"$author"}}}]):
        src_authors = list(src_as.authors.find({"used":group.get("_id")}))
        dest_as.authors.insert_many(src_authors)
        log.info("was insert %s authors rows for group %s"%(len(src_authors), group.get("_id")))
        dest_as.set_group(group.get("authors"), group.get("_id"))



if __name__ == '__main__':
    copy_data("mongodb://localhost:27017", "ae", ae_mongo_uri, ae_db_name, drop_dest=True)
