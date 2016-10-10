# coding=utf-8
import logging
import re

from wsgi.properties import shift_copy_comments_part, max_skip_comments
from wsgi.rr_people import normalize

re_url = re.compile("((https?|ftp)://|www\.)[^\s/$.?#].[^\s]*")
re_crying_chars = re.compile("[A-Z]{2,}")
re_answer = re.compile("\> ?.*\n?")
re_slash = re.compile("/?r/\S+")
re_not_latin = re.compile("[^a-zA-Z0-9\,\.\?\!\:\;\(\)\$\%\#\@\-\+\=\_\/\\\\\"\'\[\]\{\}\>\<\*\&\^\±\§\~\` \t\n\r\f]+")

log = logging.getLogger("validate")


def check_on_exclude(text, exclude_dict):
    c_tokens = set(normalize(text))
    for token in c_tokens:
        if hash(token) in exclude_dict:
            return False, None
    return True, c_tokens


def is_good_text(text):
    return len(text) >= 15 and \
           len(text) <= 140 and \
           len(re_not_latin.findall(text)) == 0 and \
           len(re_url.findall(text)) == 0 and \
           len(re_crying_chars.findall(text)) == 0 and \
           len(re_answer.findall(text)) == 0 and \
           len(re_slash.findall(text)) == 0


def get_skip_comments_count(comments_count):
    after = comments_count / shift_copy_comments_part
    if not after:
        return 0
    if after > max_skip_comments: after = max_skip_comments
    return after


def check_comment_text(text, existed_comments_texts, exclude_words):
    """
    Checking in db, and by is good and found similar text in post comments.
    Similar it is when tokens (only words) have equal length and full intersection
    :param text:
    :param post:
    :return:
    """
    try:
        if is_good_text(text):
            ok, c_tokens = check_on_exclude(text, exclude_words)
            if ok:
                for comment in existed_comments_texts:
                    p_text = comment.body
                    if is_good_text(p_text):
                        p_tokens = set(normalize(p_text))
                        if len(c_tokens) == len(p_tokens) and len(p_tokens.intersection(c_tokens)) == len(p_tokens):
                            return False, None

                return True, c_tokens
    except Exception as e:
        log.exception(e)

    return False, None
