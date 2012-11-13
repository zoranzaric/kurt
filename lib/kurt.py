import sys, os

def traverse_files(path):
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
            for file_path, file_size in traverse_files(os.path.join(path, file)):
                yield (file_path, file_size)

