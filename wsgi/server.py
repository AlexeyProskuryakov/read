# coding=utf-8
import os
import re
import signal
import time
from datetime import datetime

from flask import Flask, logging, request, render_template, session, url_for, g
from flask.json import jsonify
from flask_debugtoolbar import DebugToolbarExtension
from flask_login import LoginManager, login_user, login_required, logout_user
from werkzeug.utils import redirect

from wsgi.db import HumanStorage
from wsgi.rr_people import S_WORK, check_on_exclude
from wsgi.rr_people.entity_states import ProcessStatesPersist
from wsgi.rr_people.queue import CommentQueue
from wsgi.rr_people.reader import cs_aspect, CommentFounderStateStorage
from wsgi.rr_people.reader_manage import CommentSearcher
from wsgi.rr_people.storage import CommentsStorage, CS_COMMENTED
from wsgi.user_management import UsersHandler, User

from wsgi import properties
from wake_up.views import wake_up_app

__author__ = '4ikist'

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

import sys

reload(sys)

splitter = re.compile("[^\w\d]+")
sys.setdefaultencoding('utf-8')

log = logging.getLogger("web")
cur_dir = os.path.dirname(__file__)
app = Flask("Read", template_folder=cur_dir + "/templates", static_folder=cur_dir + "/static")

app.secret_key = 'foo bar baz'
app.config['SESSION_TYPE'] = 'filesystem'

app.register_blueprint(wake_up_app, url_prefix="/wake_up")


def tst_to_dt(value):
    return datetime.fromtimestamp(value).strftime("%H:%M %d.%m.%Y")


def to_dt(value):
    return value.strftime("%d.%m.%Y %H:%M:%S")


def array_to_string(array):
    array.sort()
    return "\n".join([str(el) for el in array])


app.jinja_env.filters['tst_to_dt'] = tst_to_dt
app.jinja_env.filters['to_dt'] = to_dt

app.jinja_env.globals.update(array_to_string=array_to_string)

if os.environ.get("test", False):
    log.info("will run at test mode")
    app.config["SECRET_KEY"] = "foo bar baz"
    app.debug = True
    app.config['DEBUG_TB_INTERCEPT_REDIRECTS'] = False
    toolbar = DebugToolbarExtension(app)

comment_searcher = CommentSearcher()

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

db = HumanStorage(name="hs server")

comment_storage = CommentsStorage("server")
comment_queue = CommentQueue("server")
state_persist = ProcessStatesPersist("server")
state_storage = CommentFounderStateStorage("server")

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
    user = g.user
    return render_template("main.html", **{"username": user.name})


@app.route("/comment_search/start/<sub>", methods=["POST"])
@login_required
def start_comment_search(sub):
    comment_queue.need_comment(sub)
    while 1:
        state = state_persist.get_process_state(cs_aspect(sub))
        if state.mutex_state:
            return jsonify({"global": state.global_state, "mutex": state.mutex_state})
        time.sleep(1)


@app.route("/comment_search/reset_state/<sub>", methods=["POST"])
@login_required
def reset_comment_searcher_state(sub):
    state_storage.reset_state(sub)
    return jsonify({"ok": True})


@app.route("/comment_search/clear_process_log/<sub>", methods=["POST"])
@login_required
def cleat_process_log(sub):
    state_persist.clear(cs_aspect(sub))
    return jsonify({"ok": True})


@app.route("/comments")
@login_required
def comments():
    subs_names = db.get_all_humans_subs()
    subs_states = {}

    for sub in subs_names:
        subs_states[sub] = state_persist.get_process_state(cs_aspect(sub))
    return render_template("comments.html", **{"subs_states": subs_states})


@app.route("/comment/<id>/<state>", methods=["POST"])
def set_comment_state(id, state):
    comment_storage.set_comment_state(id, state)
    return jsonify(**{"ok": True})


@app.route("/comments/queue/<sub>", methods=["GET"])
def sub_comments(sub):
    post_ids = comment_queue.get_all_comments_post_ids(sub)
    posts = map(lambda x: {
        "id": str(x.get("_id")),
        "url": x.get("post_url"),
        "fullname": x.get("fullname"),
        "text": x.get("text"),
        "supplier": x.get("supplier")},
                comment_storage.get_comments(post_ids))
    return jsonify(**{"posts": posts})


@app.route("/comment_search/info/<sub>")
@login_required
def comment_search_info(sub):
    posts = comment_storage.get_comments_of_sub(sub)
    comments_posts_ids = comment_queue.get_all_comments_post_ids(sub)
    posts_commented = []
    if comments_posts_ids:
        for i, post in enumerate(posts):
            post['is_in_queue'] = str(post.get("_id")) in comments_posts_ids
            posts[i] = post
            if post.get("state") == CS_COMMENTED:
                posts_commented.append(post)

    process_state = state_persist.get_process_state(cs_aspect(sub), history=True)
    cs_state = state_storage.get_state(sub)

    result = {"posts_found_comment_text": posts,
              "posts_commented": posts_commented,
              "sub": sub,
              "process_state": process_state,
              "state_history": process_state.history,
              "state": cs_state
              }
    return render_template("comment_search_info.html", **result)


@app.route("/exclude", methods=["GET", "POST"])
@login_required
def exclude():
    if request.method == "POST":
        words = request.form.get("words")
        words = filter(lambda x: x.strip(), splitter.split(words))
        comment_storage.set_words_exclude(words)
        exclude_dict = comment_storage.get_words_exclude()
        for post in comment_storage.get_comments_of_sub():
            ok, _ = check_on_exclude(post.get("text"), exclude_dict)
            if not ok:
                log.info("comment: %s \n not checked by new exclude words" % post)
                comment_storage.comments.delete_one({"_id": post.get("_id")})

    words = comment_storage.get_words_exclude()
    return render_template("exclude.html", **{"words": words.values()})


if __name__ == '__main__':
    print "listen"
    app.run(port=65010)
