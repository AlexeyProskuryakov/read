import logging
import random
import re
import string
from HTMLParser import HTMLParser

from apiclient.discovery import build

from wsgi import ConfigManager
from wsgi.properties import YOUTUBE_API_VERSION, YOUTUBE_API_SERVICE_NAME
from wsgi.rr_people.comment_suppliers import Supplier, CommentMainData
from wsgi.rr_people.comment_suppliers.utils import CommentsInPost
from wsgi.rr_people.validate import check_comment_text, get_skip_comments_count

htmlParser = HTMLParser()

y_url_re = re.compile(
    "(?:youtube(?:-nocookie)?\.com\/(?:[^\/\n\s]+\/\S+\/|(?:v|e(?:mbed)?)\/|\S*?[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})")

log = logging.getLogger("youtube_supplier")

MIN_COMMENTS_AT_YOUTUBE_POST = 20
MIN_COMMENTS_AT_REDDIT_POST = 34
MAX_COMMENTS_AT_REDDIT_POST = 100
MAX_UPS_AT_REDDIT_POST = 30


def get_video_id(post_url):
    found = y_url_re.findall(post_url)
    if found:
        return found[0]


# get videos by ids
class YoutubeCommentsSupplier(Supplier):
    def __init__(self):
        cn = ConfigManager()
        self.youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                             developerKey=cn.get('YOUTUBE_DEVELOPER_KEY'))

        self.exclude_words = {}
        self.cp = CommentsInPost()

    def get_comment_threads(self, video_id):
        try:
            results = self.youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                # textFormat="html",
                maxResults=100,
            ).execute()
            result = []
            for item in results["items"]:
                comment = item["snippet"]["topLevelComment"]
                if "parentId" not in comment["snippet"]:
                    author = comment["snippet"]["authorDisplayName"]

                    text = comment["snippet"]["textDisplay"]
                    text = ''.join(text.split(string.whitespace))
                    text = text.replace(u'\ufeff', '')
                    text = htmlParser.unescape(text)

                    published = comment["snippet"]["publishedAt"]
                    updated = comment["snippet"]["updatedAt"]
                    likes = comment["snippet"]['likeCount']

                    result.append((text, author, published, updated, likes))

            after = get_skip_comments_count(len(result))
            if after > MIN_COMMENTS_AT_YOUTUBE_POST:
                after += random.randint(-after / 5, after / 5)
            return result[after:]

        except Exception as e:
            log.warning(e)
            return []

    def set_exclude_words(self, validation):
        self.exclude_words = validation

    def get_comment(self, acceptor):
        video_id = get_video_id(acceptor.url)
        if video_id and \
                        acceptor.num_comments > MIN_COMMENTS_AT_REDDIT_POST and \
                        acceptor.num_comments < MAX_COMMENTS_AT_REDDIT_POST and \
                        acceptor.ups < MAX_UPS_AT_REDDIT_POST:

            youtube_comments = self.get_comment_threads(video_id)
            if youtube_comments:
                log.info("Will find comment for [%s][%s] in %s comments" % (
                    acceptor.fullname, acceptor.url, len(youtube_comments)))
                reddit_posts_comments = self.cp.get_comments_of_post(acceptor)
                reddit_post_author = acceptor.author
                for comment in youtube_comments:
                    text, author, _, _, likes = comment
                    if likes >= 0 and author != reddit_post_author:
                        ok, c_tokens = check_comment_text(text, reddit_posts_comments, self.exclude_words)
                        if ok:
                            return CommentMainData(acceptor.fullname,
                                                   acceptor.permalink,
                                                   text,
                                                   hash(tuple(c_tokens)))

    def get_name(self):
        return "youtube"


if __name__ == '__main__':
    ycs = YoutubeCommentsSupplier()
    for comment_thread in ycs.get_comment_threads("dqbY2k5-mbM"):
        print comment_thread
