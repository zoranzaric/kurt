import sqlite3, sys

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

