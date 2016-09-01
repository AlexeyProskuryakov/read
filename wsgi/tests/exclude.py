from wsgi.rr_people.storage import CommentsStorage


def insert_new_excluded_comments():
    cs = CommentsStorage("test")

    cs.add_ready_comment("test", 12344, "test", "nigger is fucked by pics", "not url ")
    cs.add_ready_comment("test", 12345, "test", "balls is fucked by pics", "not url ")
    cs.add_ready_comment("test", 12346, "test", "ITT is fucked by pics", "not url ")

    cs.add_ready_comment("test", 12347, "test", "nigger is fucked by pics", "not url ")
    cs.add_ready_comment("test", 12348, "test", "TIL is fucked by pics", "not url ")
    cs.add_ready_comment("test", 12349, "test", "CrInGe is fucked by pics", "not url ")

if __name__ == '__main__':
    insert_new_excluded_comments()