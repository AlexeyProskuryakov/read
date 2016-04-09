# coding=utf-8
import calendar
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from uuid import uuid4

import praw
from flask import Flask, logging, request, render_template, session, url_for, g, flash
from flask.json import jsonify
from flask_debugtoolbar import DebugToolbarExtension
from flask_login import LoginManager, login_user, login_required, logout_user
from werkzeug.utils import redirect

from wsgi.db import HumanStorage
from wsgi.properties import want_coefficient_max, DAY
from wsgi.rr_people import S_WORK, S_SUSPEND
from wsgi.rr_people.ae import AuthorsStorage
from wsgi.rr_people.he import HumanConfiguration, HumanOrchestra
from wsgi.rr_people.posting import POST_GENERATOR_OBJECTS
from wsgi.rr_people.posting.copy_gen import SubredditsRelationsStore
from wsgi.rr_people.posting.posts import PS_BAD, PS_AT_QUEUE
from wsgi.rr_people.posting.posts_generator import PostsGenerator
from wsgi.rr_people.reader import CommentSearcher, CommentsStorage
from wsgi.wake_up import WakeUp, WakeUpStorage

__author__ = '4ikist'

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

log = logging.getLogger("web")
cur_dir = os.path.dirname(__file__)
app = Flask("Humans", template_folder=cur_dir + "/templates", static_folder=cur_dir + "/static")

app.secret_key = 'foo bar baz'
app.config['SESSION_TYPE'] = 'filesystem'


def tst_to_dt(value):
    return datetime.fromtimestamp(value).strftime("%H:%M %d.%m.%Y")


def array_to_string(array):
    return " ".join([str(el) for el in array])


app.jinja_env.filters["tst_to_dt"] = tst_to_dt
app.jinja_env.globals.update(array_to_string=array_to_string)

if os.environ.get("test", False):
    log.info("will run at test mode")
    app.config["SECRET_KEY"] = "foo bar baz"
    app.debug = True
    app.config['DEBUG_TB_INTERCEPT_REDIRECTS'] = False
    toolbar = DebugToolbarExtension(app)

url = "http://rr-alexeyp.rhcloud.com"
wus = WakeUpStorage("wus server")
wus.add_url(url)

wu = WakeUp()
wu.daemon = True
wu.start()


@app.route("/wake_up/<salt>", methods=["POST"])
def wake_up(salt):
    return jsonify(**{"result": salt})


@app.route("/wake_up", methods=["GET", "POST"])
def wake_up_manage():
    if request.method == "POST":
        urls = request.form.get("urls")
        urls = urls.split("\n")
        for url in urls:
            url = url.strip()
            if url:
                wus.add_url(url)

    urls = wus.get_urls()
    return render_template("wake_up.html", **{"urls": urls})


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

db = HumanStorage(name="hs server")
comment_storage = CommentsStorage(name="cs server")


class User(object):
    def __init__(self, name, pwd):
        self.id = str(uuid4().get_hex())
        self.auth = False
        self.active = False
        self.anonymous = False
        self.name = name
        self.pwd = pwd

    def is_authenticated(self):
        return self.auth

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id


class UsersHandler(object):
    def __init__(self):
        self.users = {}
        self.auth_users = {}

    def get_guest(self):
        user = User("Guest", "")
        user.anonymous = True
        self.users[user.id] = user
        return user

    def get_by_id(self, id):
        found = self.users.get(id)
        if not found:
            found = db.users.find_one({"user_id": id})
            if found:
                user = User(found.get('name'), found.get("pwd"))
                user.id = found.get("user_id")
                self.users[user.id] = user
                found = user
        return found

    def auth_user(self, name, pwd):
        authed = db.check_user(name, pwd)
        if authed:
            user = self.get_by_id(authed)
            if not user:
                user = User(name, pwd)
                user.id = authed
            user.auth = True
            user.active = True
            self.users[user.id] = user
            return user

    def logout(self, user):
        user.auth = False
        user.active = False
        self.users[user.id] = user

    def add_user(self, user):
        self.users[user.id] = user
        db.add_user(user.name, user.pwd, user.id)


usersHandler = UsersHandler()
log.info("users handler was initted")
usersHandler.add_user(User("3030", "89231950908zozo"))


@app.before_request
def load_user():
    if session.get("user_id"):
        user = usersHandler.get_by_id(session.get("user_id"))
    else:
        # user = None
        user = usersHandler.get_guest()
    g.user = user


@login_manager.user_loader
def load_user(userid):
    return usersHandler.get_by_id(userid)


@login_manager.unauthorized_handler
def unauthorized_callback():
    return redirect(url_for('login'))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        login = request.form.get("name")
        password = request.form.get("password")
        remember_me = request.form.get("remember") == u"on"
        user = usersHandler.auth_user(login, password)
        if user:
            try:
                login_user(user, remember=remember_me)
                return redirect(url_for("main"))
            except Exception as e:
                log.exception(e)

    return render_template("login.html")


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route("/")
@login_required
def main():
    if request.method == "POST":
        _url = request.form.get("url")
        wu.what = _url

    user = g.user
    return render_template("main.html", **{"username": user.name})


REDIRECT_URI = "http://rr-alexeyp.rhcloud.com/authorize_callback"
C_ID = None
C_SECRET = None


@app.route("/humans/add_credential", methods=["GET", "POST"])
@login_required
def human_auth_start():
    global C_ID
    global C_SECRET
    global REDIRECT_URI

    if request.method == "GET":
        return render_template("human_add_credentials.html", **{"url": False})
    if request.method == "POST":
        C_ID = request.form.get("client_id")
        C_SECRET = request.form.get("client_secret")
        user = request.form.get("user")
        pwd = request.form.get("pwd")
        redirect_uri = request.form.get("redirect_uri")
        if redirect_uri:
            REDIRECT_URI = redirect_uri

        db.prepare_human_access_credentials(C_ID, C_SECRET, REDIRECT_URI, user, pwd)

        r = praw.Reddit("Hui")
        r.set_oauth_app_info(C_ID, C_SECRET, REDIRECT_URI)
        url = r.get_authorize_url("KEY",
                                  'creddits,modcontributors,modconfig,subscribe,wikiread,wikiedit,vote,mysubreddits,submit,modlog,modposts,modflair,save,modothers,read,privatemessages,report,identity,livemanage,account,modtraffic,edit,modwiki,modself,history,flair',
                                  refreshable=True)
        return render_template("human_add_credentials.html", **{"url": url})


@app.route("/authorize_callback")
@login_required
def human_auth_end():
    state = request.args.get('state', '')
    code = request.args.get('code', '')

    r = praw.Reddit("Hui")
    r.set_oauth_app_info(C_ID, C_SECRET, REDIRECT_URI)
    info = r.get_access_information(code)
    user = r.get_me()
    r.set_access_credentials(**info)
    db.update_human_access_credentials_info(user.name, info)
    return render_template("authorize_callback.html", **{"user": user.name, "state": state, "info": info, "code": code})


human_orchestra = HumanOrchestra()


@app.route("/humans", methods=["POST", "GET"])
@login_required
def humans():
    global human_orchestra

    if request.method == "POST":
        subreddits_raw = request.form.get("sbrdts")
        subreddits = subreddits_raw.strip().split()

        human_name = request.form.get("human-name")
        human_name = human_name.strip()
        log.info("Add subreddits: \n%s\n to human with name: %s" % ('\n'.join([el for el in subreddits]), human_name))

        db.set_human_subs(human_name, list(set(subreddits)))
        human_orchestra.add_human(human_name)

        return redirect(url_for('humans_info', name=human_name))

    humans_info = db.get_humans_info()
    for human in humans_info:
        human['state'] = db.get_human_state(human['user'])
    # worked_humans = map(lambda x: x.get("name"), db.get_humans_with_state(S_WORK))

    return render_template("humans_management.html",
                           **{"humans": humans_info})


@app.route("/humans/<name>", methods=["POST", "GET"])
@login_required
def humans_info(name):
    global human_orchestra

    if request.method == "POST":
        if request.form.get("stop"):
            db.set_human_state(name, S_SUSPEND)
            return redirect(url_for('humans_info', name=name))

        if request.form.get("start"):
            db.set_human_state(name, S_WORK)
            human_orchestra.add_human(name)
            return redirect(url_for('humans_info', name=name))

        config = HumanConfiguration(request.form)
        db.set_human_live_configuration(name, config)
        human_orchestra.toggle_human_config(name)

    human_log = db.get_log_of_human(name, 100)
    stat = db.get_log_of_human_statistics(name)

    human_cfg = db.get_human_config(name)
    state = db.get_human_state(name)

    return render_template("humans_info.html", **{"human_name": name,
                                                  "human_stat": stat,
                                                  "human_log": human_log,
                                                  "human_live_state": state,
                                                  "subs": human_cfg.get("subs", []),
                                                  "config": human_cfg.get("live_config") or HumanConfiguration().data,
                                                  "ss": human_cfg.get("ss", []),
                                                  "friends": human_cfg.get("frds", []),
                                                  "want_coefficient": want_coefficient_max
                                                  })


@app.route("/humans/<name>/config", methods=["POST"])
@login_required
def human_config(name):
    config_data = db.get_human_config(name)
    if config_data:
        config_data = dict(config_data)
        config_data.pop("_id")
        return jsonify(**{"ok": True, "data": config_data})
    return jsonify(**{"ok": False})


comment_searcher = CommentSearcher()
posts_generator = PostsGenerator()


@app.route("/comment_search/start/<sub>", methods=["POST"])
@login_required
def start_comment_search(sub):
    comment_searcher.start_find_comments(sub)
    while 1:
        state = comment_searcher.comment_queue.get_comment_founder_state(sub)
        if state and "work" in state:
            return jsonify({"state": state})
        time.sleep(1)


@app.route("/comments")
@login_required
def comments():
    subs_names = db.get_all_humans_subs()
    qc_s = {}
    subs = {}
    for sub in subs_names:
        queued_comments = comment_searcher.comment_queue.show_all_comments(sub)
        qc_s[sub] = queued_comments
        subs[sub] = comment_searcher.comment_queue.get_comment_founder_state(sub)

    return render_template("comments.html", **{"subs": subs, "qc_s": qc_s})


@app.route("/posts")
@login_required
def posts():
    generators_for_subs = posts_generator.queue.get_posts_generator_states()
    posts_generator.queue.get_posts_generator_states()
    qc_s = {}
    for sub in generators_for_subs.keys():
        queued_post = posts_generator.posts_storage.get_posts_for_sub(sub)
        qc_s[sub] = queued_post

    return render_template("posts.html", **{"subs": generators_for_subs, "qc_s": qc_s})


@app.route("/comment_search/info/<sub>")
@login_required
def comment_search_info(sub):
    posts = comment_storage.get_posts_ready_for_comment()
    comments = comment_searcher.comment_queue.show_all_comments(sub)
    if comments:
        for i, post in enumerate(posts):
            post['is_in_queue'] = post.get("fullname") in comments
            if post["is_in_queue"]:
                post['text'] = comments.get(post.get("fullname"), "")
            posts[i] = post

    posts_commented = comment_storage.get_posts_commented()
    subs = db.get_all_humans_subs()

    subs_states = comment_searcher.comment_queue.get_comment_founders_states()
    state = comment_searcher.comment_queue.get_comment_founder_state(sub)

    result = {"posts_found_comment_text": posts,
              "posts_commented": posts_commented,
              "sub": sub,
              "a_subs": subs,
              "subs_states": subs_states,
              "state": state}
    return render_template("comment_search_info.html", **result)


@app.route("/actions")
@login_required
def actions():
    return render_template("actions.html")


author_storage = AuthorsStorage("as server")


@app.route("/ae-represent/<name>", methods=["GET"])
@login_required
def ae_represent(name):
    def get_point_x(x):
        dt = datetime.utcnow() + timedelta(seconds=x)
        return calendar.timegm(dt.timetuple()) * 1000

    y = 3
    ssteps = author_storage.get_sleep_steps(name)
    sleep_days = defaultdict(list)
    for step in ssteps:
        sleep_days[divmod(step.get("time"), DAY)[0]].append([step['time'], step['end_time']])

    data = []
    for _, v in sleep_days.iteritems():
        avg_start = sum(map(lambda x: x[0], v)) / len(v)
        avg_end = sum(map(lambda x: x[1], v)) / len(v)

        step = (avg_end - avg_start) / 2
        x = avg_start + step
        data.append([get_point_x(x), y, step * 1000, step * 1000])

    result = {"color": "blue",
              "data": data,
              "points": {
                  "show": True,
                  "radius": 2,
                  "fillColor": "red",
                  "errorbars": "x",
                  "xerr": {"show": True, "asymmetric": True, "upperCap": "-", "lowerCap": "-"},
              }
              }
    return jsonify(**{"data": result, "ok": True})


srs = SubredditsRelationsStore("srs server")

splitter = re.compile('[^\w\d_-]*')


@app.route("/generators", methods=["GET", "POST"])
@login_required
def gens_manage():
    if request.method == "POST":
        sub = request.form.get("sub")
        generators = request.form.getlist("gens[]")
        related_subs = request.form.get("related-subs")
        key_words = request.form.get("key-words")

        related_subs = splitter.split(related_subs)
        key_words = splitter.split(key_words)

        srs.add_sub_relations(sub, related_subs)
        posts_generator.generators_storage.set_sub_gen_info(sub, generators, key_words)

        flash(u"Генераторъ постановленъ!")
    gens = POST_GENERATOR_OBJECTS.keys()
    subs = db.get_all_humans_subs()
    return render_template("generators.html", **{"subs": subs, "gens": gens})


@app.route("/generators/sub_info", methods=["POST"])
@login_required
def sub_gens_cfg():
    data = json.loads(request.data)
    sub = data.get("sub")
    related = srs.get_related_subs(sub)
    generators = posts_generator.generators_storage.get_sub_gen_info(sub)

    return jsonify(**{"ok": True, "related_subs": related, "key_words": generators.get("key_words"),
                      "generators": generators.get("gens")})


@app.route("/generators/start", methods=["POST"])
@login_required
def sub_gens_start():
    data = json.loads(request.data)
    sub = data.get("sub")
    if sub:
        posts_generator.queue.set_posts_generator_state(sub, S_WORK)
        posts_generator.start_generate_posts(sub)
        return jsonify(**{"ok": True, "state": S_WORK})
    return jsonify(**{"ok": False, "error": "sub is not exists"})


@app.route("/generators/pause", methods=["POST"])
@login_required
def sub_gens_pause():
    data = json.loads(request.data)
    sub = data.get("sub")
    if sub:
        posts_generator.queue.set_posts_generator_state(sub, S_SUSPEND, ex=3600 * 24 * 7)
        return jsonify(**{"ok": True, "state": S_SUSPEND})
    return jsonify(**{"ok": False, "error": "sub is not exists"})


@app.route("/generators/del_post", methods=["POST"])
@login_required
def del_post():
    data = json.loads(request.data)
    p_hash = data.get("url_hash")
    if p_hash:
        posts_generator.posts_storage.set_post_state(int(p_hash), PS_BAD)
        return jsonify(**{"ok": True})
    return jsonify(**{"ok": False, "error": "post url hash is not exists"})


@app.route("/generators/del_sub", methods=["POST"])
@login_required
def del_sub():
    data = json.loads(request.data)
    sub_name = data.get("sub_name")
    if sub_name:
        posts_generator.terminate_generate_posts(sub_name)
        db.remove_sub_for_humans(sub_name)
        posts_generator.posts_storage.remove_posts_of_sub(sub_name)
        posts_generator.queue.remove_post_generator(sub_name)
        return jsonify(**{"ok": True})

    return jsonify(**{"ok": False, "error": "sub is not exists"})


@app.route("/generators/prepare_for_posting", methods=["POST"])
@login_required
def prepare_for_posting():
    data = json.loads(request.data)
    sub = data.get("sub")
    if sub:
        for post in posts_generator.posts_storage.get_posts_for_sub(sub):
            posts_generator.queue.put_post_hash(sub, post)
            posts_generator.posts_storage.set_post_state(post.url_hash, PS_AT_QUEUE)
        return jsonify(**{"ok": True})

    return jsonify(**{"ok": False, "error": "sub is not exists"})


if __name__ == '__main__':
    print os.path.dirname(__file__)
    app.run(port=65010)
