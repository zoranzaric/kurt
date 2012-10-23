#!/usr/bin/env python
import sqlite3, sys, os, zipfile

MAX_PACKSIZE = 1024*1024*1024


class Db:
    """The database abstraction"""

    CREATE_FILE = """CREATE TABLE IF NOT EXISTS
    file (
    name string,
    pack number);"""
    CREATE_META = """CREATE TABLE IF NOT EXISTS
    meta (
    name string,
    key string,
    value string);"""
    SELECT_FILES = """SELECT name, pack FROM file;"""

    ADD_FILE = """INSERT INTO file VALUES (?, ?);"""
    ADD_META = """INSERT INTO meta VALUES (?, ?, ?);"""

    def __init__(self, path):
        self._db = sqlite3.connect(path)
        self._db.execute(self.CREATE_FILE)
        self._db.execute(self.CREATE_META)

    def add(self, file, pack):
        try:
            self._db.execute(self.ADD_FILE, (file, pack))
        except sqlite3.ProgrammingError:
            sys.stderr.write("Couldn't add %s\n" % file)

    def add_meta(self, file, key, value):
        try:
            self._db.execute(self.ADD_META, (file, key, value))
        except sqlite3.ProgrammingError:
            sys.stderr.write("Couldn't add metadata for %s\n" % file)

    def get_files(self):
        files = set()
        maxpack = 0
        cursor = self._db.execute(self.SELECT_FILES)
        for name, pack in cursor.fetchall():
            files.add(name)
            if pack > maxpack:
                maxpack = pack
        return files, maxpack

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


def get_packs(path, known_files):
    singles = []

    packsize = 0
    paths = []
    for file_path, file_size in get_files(path):
        if file_path[len(path)+1:] in known_files:
            continue

        if packsize + file_size <= MAX_PACKSIZE:
            paths.append(file_path)
            packsize += file_size
        elif file_size > MAX_PACKSIZE:
            singles.append(([file_path], file_size))
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

    db = Db('kurt.db')

    known_files, maxpack = db.get_files()

    for index, pack in enumerate(get_packs(PATH, known_files)):
        zipname = "pack%04d.zip" % index
        f = open(zipname, 'wb')
        z = zipfile.ZipFile(f, 'w')
        paths, packsize = pack
        print "Pack %d (%dB)" % (maxpack+index+1, packsize)
        for path in paths:
            print "  %s" % path
            pathlen = len(PATH)
            try:
                z.write(path, path[pathlen+1:])
                db.add(path[pathlen+1:], maxpack+index+1)
                file_stats = os.stat(path)
                file_size = file_stats.st_size
                db.add_meta(path[pathlen+1:], "size", file_size)
            except OSError:
                pass
            except IOError:
                pass
        z.close()
        f.close()
    db.commit()

if __name__ == "__main__":
    main()

