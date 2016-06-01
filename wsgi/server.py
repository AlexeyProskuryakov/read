# coding=utf-8
import os
import signal
import time
from datetime import datetime

from flask import Flask, logging, request, render_template, session, url_for, g
from flask.json import jsonify
from flask_debugtoolbar import DebugToolbarExtension
from flask_login import LoginManager, login_user, login_required, logout_user
from werkzeug.utils import redirect

from wsgi.db import HumanStorage
from wsgi.rr_people import S_WORK
from wsgi.rr_people.reader import CommentSearcher, cs_aspect
from wsgi.rr_people.states import get_worked_pids
from wsgi.user_management import UsersHandler, User
from wsgi.wake_up import WakeUp

__author__ = '4ikist'

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

log = logging.getLogger("web")
cur_dir = os.path.dirname(__file__)
app = Flask("Read", template_folder=cur_dir + "/templates", static_folder=cur_dir + "/static")

app.secret_key = 'foo bar baz'
app.config['SESSION_TYPE'] = 'filesystem'


def tst_to_dt(value):
    return datetime.fromtimestamp(value).strftime("%H:%M %d.%m.%Y")


def to_dt(value):
    return value.strftime("%d.%m.%Y %H:%M:%S")


def array_to_string(array):
    return " ".join([str(el) for el in array])


app.jinja_env.filters['tst_to_dt'] = tst_to_dt
app.jinja_env.filters['to_dt'] = to_dt

app.jinja_env.globals.update(array_to_string=array_to_string)

if os.environ.get("test", False):
    log.info("will run at test mode")
    app.config["SECRET_KEY"] = "foo bar baz"
    app.debug = True
    app.config['DEBUG_TB_INTERCEPT_REDIRECTS'] = False
    toolbar = DebugToolbarExtension(app)

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
                wu.store.add_url(url)

    urls = wu.store.get_urls()
    return render_template("wake_up.html", **{"urls": urls})


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

db = HumanStorage(name="hs server")
comment_searcher = CommentSearcher()
comment_storage = comment_searcher.comment_storage
comment_queue = comment_searcher.comment_queue
state_persist = comment_searcher.state_persist

usersHandler = UsersHandler(db)
log.info("users handler was initted")
usersHandler.add_user(User("3030", "89231950908zozo"))


@app.before_request
def load_user():
    if session.get("user_id"):
        user = usersHandler.get_by_id(session.get("user_id"))
    else:
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


@app.route("/comment_search/start/<sub>", methods=["POST"])
@login_required
def start_comment_search(sub):
    comment_queue.need_comment(sub)
    while 1:
        state = state_persist.get_state(cs_aspect(sub))
        if state.mutex_state and S_WORK in state.mutex_state:
            return jsonify({"global": state.global_state, "mutex": state.mutex_state, "history": state.history})
        time.sleep(1)


@app.route("/comment_search/reset_state/<sub>", methods=["POST"])
@login_required
def reset_comment_searcher_state(sub):
    comment_searcher.state_storage.reset_state(sub)
    return jsonify({"ok": True})


@app.route("/comments")
@login_required
def comments():
    subs_names = db.get_all_humans_subs()
    subs_states = {}
    wp = get_worked_pids()
    for sub in subs_names:
        subs_states[sub] = state_persist.get_state(cs_aspect(sub), worked_pids=wp)

    return render_template("comments.html", **{"subs_states": subs_states})


@app.route("/comments/queue/<sub>", methods=["GET"])
def sub_comments(sub):
    post_ids = comment_queue.get_all_comments_post_ids(sub)
    posts = map(lambda x: {"url": x.get("post_url"), "fullname": x.get("fullname"), "text": x.get("text")},
                comment_storage.get_posts(post_ids))
    return jsonify(**{"posts": posts})


@app.route("/comment_search/info/<sub>")
@login_required
def comment_search_info(sub):
    posts = comment_storage.get_posts_ready_for_comment(sub)
    comments_posts_ids = comment_queue.get_all_comments_post_ids(sub)
    if comments_posts_ids:
        for i, post in enumerate(posts):
            post['is_in_queue'] = post.get("fullname") in comments_posts_ids
            posts[i] = post

    posts_commented = comment_storage.get_posts_commented(sub)
    subs = db.get_all_humans_subs()

    text_state = state_persist.get_state(cs_aspect(sub), history=True)
    state = comment_searcher.state_storage.get_state(cs_aspect(sub))

    result = {"posts_found_comment_text": posts,
              "posts_commented": posts_commented,
              "sub": sub,
              "a_subs": subs,
              "text_state": text_state,
              "state_history": text_state.history,
              "state": state
              }
    return render_template("comment_search_info.html", **result)


if __name__ == '__main__':
    print os.path.dirname(__file__)
    import random

    port = random.randint(65000, 65100)
    print "PORT: ", port
    app.run(port=port)
