#!/usr/bin/env python
import sqlite3, sys, os

MAX_PACKSIZE = 1024*1024*1024


class Db:
    """The database abstraction"""

    CREATE = """CREATE TABLE IF NOT EXISTS
    file (
    name string,
    pack number);"""

    ADD = """INSERT INTO file VALUES (?, ?);"""

    def __init__(self, path):
        self._db = sqlite3.connect(path)
        self._db.execute(self.CREATE)

    def add(self, file, pack):
        try:
            self._db.execute(self.ADD, (file, pack))
        except sqlite3.ProgrammingError:
            sys.stderr.write("Couldn't add %s\n" % file)

    def commit(self):
        self._db.commit()


def usage():
    sys.stderr.write("usage: kurt.py <path>\n")


def get_files(path):
    try:
        files = os.listdir(path)
    except OSError:
        sys.stderr.write("Couldn't traverse %s\n" % path)
        files = []

    for file in sorted(files):
        if os.path.isfile(os.path.join(path, file)):
            file_path = os.path.join(path, file)
            try:
                file_stats = os.stat(file_path)
                file_size = file_stats.st_size
            except OSError:
                file_size = 0
            yield (file_path, file_size)
        if os.path.isdir(os.path.join(path, file)):
            for file_path, file_size in get_files(os.path.join(path, file)):
                yield (file_path, file_size)


def get_packs(path):
    singles = []

    packsize = 0
    paths = []
    for path, size in get_files(path):
        if packsize + size <= MAX_PACKSIZE:
            paths.append(path)
            packsize += size
        elif size > MAX_PACKSIZE:
            singles.append(([path], size))
        else:
            yield (paths, packsize)
            packsize = 0
            paths = []

            for single in singles:
                yield single
            singles = []
    if len(paths) > 0:
        yield (paths, packsize)


def main():
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)

    PATH = sys.argv[1]
    #TODO check if path exists

    db = Db(os.path.join(PATH, 'kurt.db'))

    for index, pack in enumerate(get_packs(PATH)):
        paths, packsize = pack
        print "Pack %d (%dB)" % (index, packsize)
        for path in paths:
            print "  %s" % path
            pathlen = len(PATH)
            db.add(path[pathlen+1:], index)
    db.commit()

if __name__ == "__main__":
    main()

