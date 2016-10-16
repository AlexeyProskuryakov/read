# coding=utf-8
import logging
import os
import sys

__author__ = 'alesha'


# import urllib3.contrib.pyopenssl
# urllib3.contrib.pyopenssl.inject_into_urllib3()

def module_path():
    if hasattr(sys, "frozen"):
        return os.path.dirname(
            sys.executable
        )
    return os.path.dirname(__file__)


cacert_file = os.path.join(module_path(), 'cacert.pem')

logger = logging.getLogger()

logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s[%(levelname)s]%(name)s|%(processName)s(%(process)d): %(message)s')
formatter_process = logging.Formatter('%(asctime)s[%(levelname)s]%(name)s|%(processName)s: %(message)s')
formatter_human = logging.Formatter('%(asctime)s[%(levelname)s]%(name)s|%(processName)s: %(message)s')

sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)


logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

expire_low_copies_posts = 3600 * 24 * 7

states_address = "https://read-shlak0bl0k.rhcloud.com/rockmongo/x"

states_user = "admin"
states_pwd = "YsrSQnuBJGhH"

osmdp = os.environ.get("OPENSHIFT_MONGODB_DB_PORT", 27017)
osmdh = os.environ.get("OPENSHIFT_MONGODB_DB_HOST", "localhost")
states_conn_url = "mongodb://%s:%s@%s:%s/read" % (states_user, states_pwd, osmdh, osmdp)
print "states credentials: ", osmdh, osmdp, states_user, states_pwd, '\n', states_conn_url
states_db_name = "read"

SEC = 1
MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24
WEEK = DAY * 7
WEEK_DAYS = {0: "MO", 1: "TU", 2: "WE", 3: "TH", 4: "FR", 5: "SA", 6: "SU"}

TIME_TO_WAIT_NEW_COPIES = 3600 * 24
TIME_TO_RELOAD_SUB_POSTS = 3600 * 2  # at live random in consumer


DEFAULT_LIMIT = 500
# DEFAULT_LIMIT = 20
DEFAULT_SLEEP_TIME_AFTER_GENERATE_DATA = 60 * 60 * 4

min_copy_count = 3
min_comment_create_time_difference = 3600 * 24 * 10

shift_copy_comments_part = 5  # общее количество комментариев / это число  = сколько будет пропускаться
min_donor_comment_ups = 5
max_donor_comment_ups = 100000
min_donor_num_comments = 50
max_skip_comments = 34


YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# imgur properties

test_mode = os.environ.get("RR_TEST", "false").strip().lower() in ("true", "1", "yes")
print "TEST? ", test_mode

WORKED_PIDS_QUERY = os.environ.get("WORKED_PIDS_QUERY", "python")

# logger.info(
#     "Reddit People MANAGEMENT SYSTEM STARTED... \nEnv:%s" % "\n".join(["%s:\t%s" % (k, v) for k, v in os.environ.iteritems()]))
