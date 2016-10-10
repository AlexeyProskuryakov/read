from wsgi.rr_people.storage import CommentsStorage


def insert_new_excluded_comments():
    cs = CommentsStorage("test")

    cs.add_ready_comment("test", 12344, "nigger is fucked by pics", "not url ", "test")
    cs.add_ready_comment("test", 12345, "balls is fucked by pics", "not url ", "test")
    cs.add_ready_comment("test", 12346, "ITT is fucked by pics", "not url ", "test")

    cs.add_ready_comment("test", 12347, "nigger is fucked by pics", "not url ", "test")
    cs.add_ready_comment("test", 12348, "TIL is fucked by pics", "not url ", "test")
    cs.add_ready_comment("test", 12349, "CrInGe is fucked by pics", "not url ", "test")

if __name__ == '__main__':
    insert_new_excluded_comments()