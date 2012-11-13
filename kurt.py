#!/usr/bin/env python
import sys, os, zipfile
import options
from db import Db

MAX_PACKSIZE = 1024*1024*1024

OPTSPEC = """
kurt.py [options] <path>
--
 Options:
o,output= path to the output directory
v,verbose increase log output (can be used more than once)
"""


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
    o = options.Options(OPTSPEC)
    (opt, flags, extra) = o.parse(sys.argv[1:])

    if not extra:
        o.fatal('no path given')

    PATH = extra[0]
    if not os.path.exists(PATH):
        o.fatal('path <%s> does not exist' % PATH)

    if opt.output and not os.path.exists(opt.output):
        o.fata('output-path <%s> does not exist' % opt.output)

    if opt.output:
        metapath = os.path.join(opt.output, 'meta')
    else:
        metapath = 'meta'
    if not os.path.exists(metapath):
        os.makedirs(metapath)

    db = Db(os.path.join(metapath, 'kurt.db'))

    known_files, maxpack = db.get_files()

    for index, pack in enumerate(get_packs(PATH, known_files)):
        zipname = "pack%04d.zip" % index
        if opt.output:
            zippath = os.path.join(opt.output, zipname)
        else:
            zippath = zipname
        f = open(zippath, 'wb')
        z = zipfile.ZipFile(f, 'w')

        paths, packsize = pack
        if opt.verbose:
            print "Pack %d (%dB)" % (maxpack+index+1, packsize)
        for path in paths:
            if opt.verbose:
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

